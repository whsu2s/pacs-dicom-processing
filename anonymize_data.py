from datetime import datetime
import os
import pydicom
from pydicom.datadict import tag_for_keyword
import hashlib
import multiprocessing as mp
import warnings
warnings.filterwarnings("ignore")

# SPECIFY DIRECTORIES
DATADIR = "../dicom"
SRCDIR = os.path.join(DATADIR, "extracted")
DSTDIR = os.path.join(DATADIR, "anonymized")


def anonymize_patient_id(patient_id):
    # Use a hash function to create a unique anonymized patient ID
    anonymized_id = hashlib.sha256(patient_id.encode()).hexdigest()[:10]
    return anonymized_id


def anonymize_dicom(dicom_file, output_file, anonymized_id):
    # Load the DICOM file
    ds = pydicom.dcmread(dicom_file)

    # Remove personal information (Patient Name, Patient ID, etc.)
    ds.PatientID = anonymized_id
    ds.PatientName = "ANONYMIZED"

    # Check for any additional private tags and remove them
    ds.PatientBirthDate = ''
    ds.PatientAddress = ''

    # Tags to remove, based on the DICOM standard for de-identification (Table E.1-1)
    tags_to_remove = [
        'OtherPatientIDs', 'OtherPatientNames', 'PatientBirthDate', 'PatientBirthTime',
        'PatientSex', 'PatientAge', 'PatientAddress', 'PatientMotherBirthName',
        'PatientTelephoneNumbers', 'PatientInsurancePlanCodeSequence', 'PatientPrimaryLanguageCodeSequence',
        'ResponsiblePerson', 'ResponsiblePersonRole', 'ResponsibleOrganization',
        'IssuerOfPatientID', 'IssuerOfPatientIDQualifiersSequence', 'PatientID',
        'AccessionNumber', 'InstitutionName', 'InstitutionAddress',
        'InstitutionalDepartmentName', 'ReferringPhysicianName', 'ReferringPhysicianTelephoneNumbers',
        'PhysiciansOfRecord', 'PerformingPhysicianName', 'OperatorsName',
        'StudyDate', 'SeriesDate', 'AcquisitionDate', 'ContentDate',
        'StudyTime', 'SeriesTime', 'AcquisitionTime', 'ContentTime',
        'StudyID', 'StudyDescription', 'SeriesDescription', 'PhysicianOfRecord',
        'PerformingPhysicianName', 'DeviceSerialNumber', 'InstitutionalDepartmentName',
        'StationName', 'Manufacturer', 'ManufacturerModelName', 'SoftwareVersions',
        'ProtocolName', 'DeviceUID', 'StationName', 'ScheduledProcedureStepDescription'
    ]

    # Remove or anonymize sensitive information
    for tag_keyword in tags_to_remove:
        tag = tag_for_keyword(tag_keyword)
        if tag in ds:
            del ds[tag]
    ds.remove_private_tags()

    # Remove UIDs
    ds.StudyInstanceUID = ''
    ds.SeriesInstanceUID = ''
    ds.SOPInstanceUID = ''


    # Save the anonymized DICOM file
    # Save the anonymized file to the output directory
    ds.save_as(output_file)
    return f"Anonymized: {dicom_file} -> {output_file}"


def process_dicom(dicom_file, output_file, anonymized_id):
    return anonymize_dicom(dicom_file, output_file, anonymized_id)


def anonymize_dicom_files(patient_folder, anonymized_id):
    ''' Iterate over patient folder and anonymize all dicom files.
    Args:
        patient_folder: Folder with 10-digit patient ID as prefix
        anonymized_id: 10-digit hash
    Return:
        None
    '''
    patient_path = os.path.join(SRCDIR, patient_folder)

    # Iterate through study folders
    for study_folder in os.listdir(patient_path):
        study_path = os.path.join(patient_path, study_folder)

        # Iterate through series folders
        for series_folder in os.listdir(study_path):
            series_path = os.path.join(study_path, series_folder)

            # Iterate through DICOM files
            for dicom_file in os.listdir(series_path):
                dicom_file_path = os.path.join(series_path, dicom_file)

                # Ensure it's a file and ends with ".dcm"
                if os.path.isfile(dicom_file_path) and dicom_file.endswith(".dcm"):

                    # Create corresponding output directories (with anonymized patient ID)
                    output_patient_folder = os.path.join(DSTDIR, anonymized_id)
                    output_study_folder = os.path.join(output_patient_folder, study_folder)
                    output_series_folder = os.path.join(output_study_folder, series_folder)
                    os.makedirs(output_series_folder, exist_ok=True)

                    # Output file path
                    output_file_path = os.path.join(output_series_folder, dicom_file)

                    # Anonymize dicom file
                    process_dicom(dicom_file_path, output_file_path, anonymized_id)


def main():
    patient_lookup = {}

    # 1) Create a lookup table
    for i, patient_folder in enumerate(os.listdir(SRCDIR)):
        PatientID = patient_folder[:10]

        print(f'Processing {i} - {PatientID}')
        print('-'*20)
        print()

        anonymized_id = anonymize_patient_id(PatientID)
        patient_lookup[PatientID] = anonymized_id
        print(PatientID, anonymized_id)
    anonymized_ids = list(patient_lookup.values())

    # 2) Anonymize files using multiprocessing
    patient_folders = os.listdir(SRCDIR)
    with mp.Pool(mp.cpu_count()) as pool:
        _ = pool.starmap(anonymize_dicom_files, zip(patient_folders, anonymized_ids))


if __name__ == "__main__":
    start = datetime.now()
    main()
    print(f'Running time: {datetime.now() - start}')
