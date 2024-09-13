from datetime import datetime, timedelta


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
