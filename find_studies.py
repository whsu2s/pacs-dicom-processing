import click
import pandas as pd
from datetime import datetime, timedelta
import time
from pydicom.dataset import Dataset
from pynetdicom import AE, debug_logger
from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelFind

from filter_items import *
from utils import calculate_previous_date, convert_date_format

# Parameters
IP = ""
PORT = 
TODAY = ''.join(str(datetime.now())[:10].split('-'))
AEC = {'ip': IP, 'port': PORT}
AET = ""
AE_TITLE = ""


def call_PACS(date, days, level, pid, protocol, filter):
    ''' Call GEPACS to get data information.
    '''
    # Params to store data
    study_dates = []
    accession_numbers = []
    study_descriptions = []
    patient_ids = []
    body_parts = []
    study_uids = []
    series_uids = []
    series_descriptions = []
    ages = []

    # Iterate over days from the specified date
    date0 = date
    for i in range(days):
        if i == 0:
            date = date0
        else:
            date = calculate_previous_date(date0, days=i)

        print('-'*20)
        print(f'{i}: {date}')

        # Create a DICOM query dataset
        ds = Dataset()
        ds.StudyDate = date              # 0008,0020
        ds.AccessionNumber = ""          # 0008,0050
        ds.StudyDescription = ""         # 0008,1030
        ds.Modality = "MR"               # 0008,0060
        ds.PatientID = pid               # 0010,0020
        ds.BodyPartExamined = ""         # 0018,0015
        ds.QueryRetrieveLevel = level.upper()  # 0008,0052
        ds.StudyInstanceUID = ""         # 0020,000d
        ds.PatientBirthDate = ""         # 0010,0030

        if level == 'series':
            ds.SeriesInstanceUID = ""        # 0020,000e
            ds.SeriesDescription = ""        # 0008,103e

        # Create association with the PACS server
        ae = AE(ae_title=AE_TITLE)
        ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)  # Add the relevant DICOM service UID for query/retrieve

        # Start association with the PACS 
        assoc = ae.associate(AEC['ip'], AEC['port'], ae_title=b"GEPACS")

        if assoc.is_established:
            # Use C-FIND to query the PACS
            responses = assoc.send_c_find(ds, StudyRootQueryRetrieveInformationModelFind) 
          
            for j, (status, identifier) in enumerate(responses):
                if status:
                    #print("C-FIND Query Status: 0x{0:04x}".format(status.Status))
                    # 'identifier' contains the dataset with retrieved information
                    # Process the retrieved DICOM dataset as needed
                    if identifier is not None:
                        # Check if the study contains brain scans
                        body_part = identifier.get('BodyPartExamined', 'N/A')
                        study_description = identifier.get('StudyDescription', 'N/A')
                        study_date = identifier.get('StudyDate')
                        bd = identifier.get('PatientBirthDate')
                        yy, mm, dd = int(study_date[:4]), int(study_date[4:6]), int(study_date[6:])
                        age = convert_date_format(study_date).year - convert_date_format(bd).year

                        # Only find series of neuroimaging of adults
                        if body_part in ['BRAIN', 'HEAD', 'SKULL'] and age >= 18:
                            print('Response: ', body_part, study_date, age)

                            # Series level: keep items if series contain anat, func or diff series
                            if level == 'series':
                                series_item = identifier.get('SeriesDescription', 'N/A')
                                if filter:
                                    if (series_item in anat_series) or (series_item in func_series) or (series_item in diff_series):
                                        series_uids.append(identifier.get('SeriesInstanceUID', 'N/A'))
                                        series_descriptions.append(series_item)
                                    else:
                                        continue
                                else:
                                    series_uids.append(identifier.get('SeriesInstanceUID', 'N/A'))
                                    series_descriptions.append(series_item)

                            # Append data
                            study_dates.append(study_date)
                            accession_numbers.append(identifier.get('AccessionNumber', 'N/A'))
                            study_descriptions.append(identifier.get('StudyDescription', 'N/A'))
                            patient_ids.append(identifier.get('PatientID', 'N/A'))
                            body_parts.append(body_part)
                            study_uids.append(identifier.get('StudyInstanceUID', 'N/A'))
                            ages.append(age)

                else:
                    print("C-FIND Failed")

            # Release the association
            assoc.release()
        else:
            print("Association rejected, aborted or never connected!")

        # Pause for some time
        time.sleep(2)

    # Create text file
    df = pd.DataFrame(
        list(zip(study_dates, accession_numbers, patient_ids, ages, body_parts, study_descriptions, study_uids)),
        columns=['StudyDate', 'AccessionNumber', 'PatientID', 'Age', 'BodyPartExamined', 'StudyDescription', 'StudyInstanceUID']
    )
    if level == 'series':
        df['SeriesDescription'] = series_descriptions
        df['SeriesInstanceUID'] = series_uids
    df.to_excel(f'studies_available-{protocol}-{level}-{date0}-{days}.xlsx')


@click.command()
@click.option('--date', default=TODAY, type=str, help='The date to start data retrieval.')
@click.option('--days', default=1, type=int, help='The number of days from the specified date backward.')
@click.option('--level', default='series', type=str, help='The query level: "study" or "series".')
@click.option('--pid', default='', type=str, help='The patient ID.')
@click.option('--protocol', default=None, type=str, help=f'The disease protocol used for image acquisition: {PROTOCOLS}.')
@click.option('--filter', default=False, type=bool, help='Whether to filter specific sequences.')
def main(date, days, level, pid, protocol, filter):
    # Date format
    assert len(date) == 8, print('The date format should be yyyymmdd.')
    assert level in ['study', 'series'], print('Choose a query level: "study" or "series".')

    # Retrieve data
    print('='*60)
    print(f'Retrieving MR data from GEPACS, starting from {date} backward...')
    print('='*60)
    call_PACS(date, days, level, pid, protocol, filter)


if __name__ == "__main__":
    start = datetime.now()
    main(standalone_mode=False)
    print(f'Running time: {datetime.now() - start}')
