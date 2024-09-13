from datetime import datetime, timedelta
import time
import json
import os
from pydicom.dataset import Dataset
from pynetdicom import AE, debug_logger
from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelFind
import requests
import shutil
import subprocess
import warnings
import zipfile


# Parameters
IP = ""
PORT = 4100
TODAY = ''.join(str(datetime.now())[:10].split('-'))
AEC = {'ip': IP, 'port': PORT}
AET = ""

TMP_DIR = "../../tmp/dicom"
OUT_DIR = "../../tmp/dicom/data"
URL = ""
headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
}
proxies = {
    'https': '',
    'http': ''
}
AE_TITLE = ""
AUTH = ('', '')


"""
    Date related function
"""
def calculate_previous_date(date, days=1):
    ''' Given a date yyyymmdd, return the previous date.
    '''
    year, month, day = int(date[:4]), int(date[4:6]), int(date[6:])
    previous_date = datetime(year, month, day) - timedelta(days=days)
    previous_date = ''.join(str(previous_date)[:10].split('-'))
    return previous_date


def convert_date_format(date):
    ''' Convert six-digit date to datetime format.
    '''
    if date is None or len(date) != 8:  # Handle non date
        date = TODAY
    yy, mm, dd = int(date[:4]), int(date[4:6]), int(date[6:])
    return datetime(yy, mm, dd)


"""
    PACS functions
"""
def retrieve_data(pid, sdate, print_sequences=True, return_response=False):
    """ Print all available sequences given patient ID and study date.
    Args:
        pid (str): patient ID
        sdate (str): study date
    """
    patientID = int(pid)
    studyDate = sdate
    identifiers = []

    # Check data by patient ID and study date, and retrieve series ID
    cmd = f"findscu -S -aet {AET} -k 0008,0052='STUDY' -k 0008,0060='MR' -k 0010,0020={patientID:010d} \
            -k 0008,0020={studyDate} -k 0010,0030= -aec GEPACS {AEC['ip']} {AEC['port']} 2>&1 | strings | grep 0020,000d"
    result = subprocess.run([cmd], shell=True, capture_output=True, text=True)
    if len(result.stdout) > 0:
        print(result.stdout)

    # Retrieve data
    ds = Dataset()
    ds.StudyDate = studyDate            # 0008,0020
    ds.StudyDescription = ""            # 0008,1030
    ds.Modality = "MR"                  # 0008,0060
    ds.PatientID = f'{patientID:010d}'          # 0010,0020
    ds.BodyPartExamined = ""            # 0018,0015
    ds.QueryRetrieveLevel = "SERIES"    # 0008,0052
    ds.StudyInstanceUID = ""            # 0020,000d
    ds.SeriesInstanceUID = ""           # 0020,000e
    ds.SeriesDescription = ""           # 0008,103e

    # Create association with the PACS server
    ae = AE(ae_title=AE_TITLE)
    ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)  # Add the relevant DICOM service UID for query/retrieve

    # Start association with the PACS at IP 172.20.174.148 4100
    assoc = ae.associate(AEC['ip'], AEC['port'], ae_title=b"GEPACS")

    if assoc.is_established:
        # Use C-FIND to query the PACS
        responses = assoc.send_c_find(ds, StudyRootQueryRetrieveInformationModelFind)  # "P" indicates StudyRootQueryRetrieveInformationModelFind

        for j, (status, identifier) in enumerate(responses):
            if status:
                if identifier is not None:
                    # Check if the study contains brain scans
                    study_description = identifier.get('StudyDescription', 'N/A')
                    study_date = identifier.get('StudyDate')
                    bd = identifier.get('PatientBirthDate')
                    SerInsUID = identifier.get('SeriesInstanceUID')

                    series_item = identifier.get('SeriesDescription', 'N/A')

                    if print_sequences:
                        print(series_item, SerInsUID)

                    if return_response:
                        identifiers.append(identifier)

            else:
                print("C-FIND Failed")

        # Release the association
        assoc.release()
    else:
        print("Association rejected, aborted or never connected!")

    # Pause for some time
    time.sleep(2)

    if return_response:
        return identifiers


def move_data(StuInsUID, SerInsUID, AEM):
    """ Use MOVESCU to copy data from PACS to a specified AEM
    Args:
        StuInsUID:  study instance UID
        SerInsUID: series instance UID
        AEM: Move destination AE title
    Return:
        None
    """
    cmd = f"movescu --debug --port 1234 --max-pdu 64234 -aem {AEM} -S \
    -k 0008,0052='SERIES' -k 0020,000d='{StuInsUID}' -k 0020,000E='{SerInsUID}' \
    -aet {AET} -aec GEPACS {AEC['ip']} {AEC['port']}"
    _ = subprocess.run(cmd, shell=True, capture_output=False, text=True)


def transfer_data(patID, SerInsUID, StuDate):
    """ Transfer data from the specified AEM to PC.
    Args:
        patID: patient ID
        SerInsUID: series instance UID
        StuDate: study date
    """
    data = {
        "Level" : "Series",
        "Query" : {"PatientID" : str(patID).zfill(10), "SeriesInstanceUID" : SerInsUID}
    }
    # Convert data to JSON format
    data_json = json.dumps(data)

    # Suppress the InsecureRequestWarning
    warnings.filterwarnings("ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning)

    url1 = URL + '/tools/find'
    response = requests.post(
        url1,
        headers=headers,
        data=data_json,
        verify=False,
        auth=AUTH,
        proxies=proxies
    )

    # Check response
    if response.status_code == 200:
        print("Request successful!")
        print("Response:", response.json())  # Print the response content
        if response.json() is not None and len(response.json()) > 0:
            fname = response.json()[0]
        else:
            print("Response is none.")
            return
    else:
        print("Request failed with status code:", response.status_code)
        print("Response:", response.text)
        return

    # 3) Make a GET request (wget) to download file
    url2 = f'{URL}//series/{fname}/archive'
    response = requests.get(
        url2,
        auth=AUTH,
        proxies=proxies,
        verify=False
    )

    # Check response
    if response.status_code == 200:
        # Save the content to a file
        tmp_file = os.path.join(TMP_DIR, f'{fname}.zip')
        with open(tmp_file, 'wb') as file:
            file.write(response.content)
        print("Download successful!")
    else:
        print("Download failed with status code:", response.status_code)
        print("Response:", response.text)  # Print the error message or response content

    # Unzip the downloaded file
    tmp_file = os.path.join(TMP_DIR, f'{fname}.zip')
    with zipfile.ZipFile(tmp_file, 'r') as zip_ref:
        zip_ref.extractall(os.path.join(TMP_DIR, 'extracted'))  # Extract the contents to a folder
    #print(os.listdir(os.path.join(TMP_DIR, 'extracted')))

    # 4) Get dcm files
    extracted_dir = os.path.join(TMP_DIR, 'extracted')
    top_dir = os.listdir(extracted_dir)[0]
    dcm_dir = os.path.join(extracted_dir, top_dir)
    
    # 5) Rename, remove files
    # Rename top directory
    dir_name1 = f'{patID}-{StuDate}'
    os.rename(dcm_dir, os.path.join(os.path.dirname(dcm_dir), dir_name1))
    dcm_dir = os.path.join(extracted_dir, dir_name1)
    
    # Rename second directory
    second_dir = os.listdir(dcm_dir)[0]
    series_dir = os.path.join(dcm_dir, second_dir)
    dir_name2 = SerInsUID
    os.rename(series_dir, os.path.join(os.path.dirname(series_dir), dir_name2))
    series_dir = os.path.join(dcm_dir, dir_name2)
    print(os.listdir(dcm_dir), os.listdir(os.path.join(dcm_dir, dir_name2)))
    last_dir = os.listdir(series_dir)[0]
    dcm_files = os.listdir(os.path.join(series_dir, last_dir))

    Move dcm files to the parent directory
    print('Moving files')
    for f in dcm_files:
        source = os.path.join(series_dir, last_dir, f)
        dest = os.path.join(series_dir, f)
        if os.path.isfile(source):
            shutil.move(source, dest)
    
    # Move data to sourcedata directory
    target_dir = os.path.join(OUT_DIR, dir_name1, dir_name2)
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
        for f in os.listdir(series_dir):
            shutil.move(os.path.join(series_dir, f), target_dir)
    
        print(f'Moved {dir_name1}')
    
    
    # Remove file
    shutil.rmtree(dcm_dir)
    os.remove(tmp_file)
