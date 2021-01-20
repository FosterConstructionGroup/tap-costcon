from os import listdir, path
from datetime import datetime
import hashlib
import csv
import pytz


def list_files(folder_path):
    paths = [path.join(folder_path, fl) for fl in listdir(path.join(folder_path))]
    return [(p, path.getmtime(p)) for p in paths]


time_format = "%Y-%m-%dT%H:%M:%SZ"


def parse_date(dt, format=time_format):
    return datetime.strptime(dt, format)


def format_date(dt, format=time_format):
    return datetime.strftime(dt, format)


def get_time():
    tz = pytz.timezone("Pacific/Auckland")
    now = datetime.now()
    return tz.localize(now)


def hash(s):
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def try_float(s, fallback=None):
    try:
        # Python can't parse strings that contain commas
        return float(s.replace(",", ""))
    except:
        return fallback


def parse_csv(path, delimiter=",", mappings={}, skip=0):
    with open(path, encoding="utf-8-sig") as contents:
        parsed = [row for row in csv.reader(contents, delimiter=delimiter)]
        headers = [
            transform_column_name(mappings.get(h.strip(), h)) for h in parsed[skip]
        ]
        rows = parsed[(skip + 1) :]
        return [dict(zip(headers, r)) for r in rows]


def transform_record(properties, record, date_format="%d/%m/%Y", trim_columns=[]):
    for key in record:
        if key in trim_columns:
            record[key] = record[key][:1000]

        if key in properties:
            prop = properties.get(key)
            # numbers come through as strings
            if prop.get("type")[-1] == "number":
                record[key] = None if record[key] == "" else float(record[key])

            if prop.get("format") == "date":
                if record[key] == "" or record[key] == "00/00/00":
                    record[key] = None
                else:
                    try:
                        dt = parse_date(record[key], date_format)
                        # %04Y zero-pads years like 216 to 0216 so they don't fail SQL ingestion
                        record[key] = (
                            None if dt.year < 2000 else format_date(dt, "%04Y-%m-%d")
                        )
                    except:
                        record[key] = None

    return record


def get_abs_path(p):
    return path.join(path.dirname(path.realpath(__file__)), p)


def transform_column_name(s):
    return s.strip().replace(" ", "_").lower().replace("/", "_or_")
