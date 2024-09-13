import click
from datetime import datetime
import pandas as pd
from utils import retrieve_data, move_data, transfer_data

AEM = ""  
EXCEL_TAB = "stroke"


@click.command()
@click.option('--file', type=str, required=True, help='The excel file for patient selection or data retrieval.')
def main(file):
    # 1) Read data from file
    df = pd.read_csv(file)
    #df = pd.read_excel(file, EXCEL_TAB)  
    print(df.head())

    # 2) Retrieve series instance UID
    for i, row in df.iterrows():
        # Get patient study info
        patientID = int(row['Patient'])
        studyDate = str(row['MRI_StudyDate'])[:10].replace('-', '')
        print(i, patientID)

        response = retrieve_data(patientID, studyDate, print_sequences=False, return_response=True)

        # Check retrieved response
        if len(response) > 0:
            for j, res in enumerate(response):
                study_description = res.get('StudyDescription', 'N/A')
                study_date = res.get('StudyDate')
                StuInsUID = res.get('StudyInstanceUID')
                SerInsUID = res.get('SeriesInstanceUID')
                series_item = res.get('SeriesDescription', 'N/A')

                # 3) Move data from PACS
                print(f'Transfering {series_item} ({patientID:010d}/{studyDate})')
                print('='*20)
                move_data(StuInsUID, SerInsUID, AEM=AEM)

                # 4) Transfer data (from IDIRRESEARCH to MEDPHYS)
                print('Transferring data ...')
                print('='*20)
                transfer_data(patientID, SerInsUID, studyDate)


if __name__ == "__main__":
    start = datetime.now()
    main(standalone_mode=False)
    print(f'Running time: {datetime.now() - start}')
