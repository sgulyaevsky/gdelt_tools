import datetime
from zipfile import ZipFile


def timedate_to_timestamp(timedate):
    year = int(timedate[:4])
    month = int(timedate[4:6])
    day = int(timedate[6:8])
    hour = int(timedate[8:10])
    minutes = int(timedate[10:12])
    dt = datetime.datetime(year, month, day, hour, minutes, 00)
    return dt.replace(tzinfo=datetime.timezone.utc).timestamp()


def timedate_to_days(timedate):
        return round(timedate_to_timestamp(timedate)/(3600*24), 4)


def file_to_lines(events_filename):
    with ZipFile(events_filename) as zip_file:
            with zip_file.open(zip_file.namelist()[0]) as csv_file:
                csv_string = csv_file.read().decode()
                return [line for line in csv_string.split('\n') if len(line) > 10]
