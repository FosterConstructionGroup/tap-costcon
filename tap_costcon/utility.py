from os import listdir, path
from datetime import datetime, date
import hashlib
import csv
import pytz


def list_files(folder_path):
    paths = [path.join(folder_path, fl) for fl in listdir(path.join(folder_path))]
    files = [(p, path.getmtime(p)) for p in paths]
    # reversed order means most recent files first; can couple this with a `set` to track already-seen rows instead of a `dict` to deduplicate
    files.sort(key=lambda x: x[1], reverse=True)
    return files


time_format = "%Y-%m-%dT%H:%M:%SZ"


def parse_date(dt, format=time_format):
    return datetime.strptime(dt, format)


def format_date(dt, format=time_format):
    return datetime.strftime(dt, format)


def construct_date(year, month, day):
    return date(year, month, day)


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


# generator to reduce memory consumption
def parse_csv(path, delimiter=",", mappings={}):
    with open(path, encoding="utf-8-sig") as contents:
        reader = csv.reader(contents, delimiter=delimiter)
        headers = [
            transform_column_name(mappings.get(h.strip(), h)) for h in next(reader)
        ]

        for row in reader:
            yield dict(zip(headers, row))


def transform_record(properties, record, date_format="%d/%m/%Y", trim_columns=[]):
    for key in record:
        val = record[key]

        if key in trim_columns:
            record[key] = val[:1000]

        prop = properties.get(key)
        if prop:
            # numbers come through as strings
            if prop.get("type")[-1] == "number":
                record[key] = None if val == "" else float(record[key])
            elif prop.get("format") == "date":
                if val == "" or val == "00/00/00":
                    record[key] = None
                else:
                    try:
                        dt = parse_date(val, date_format)
                        # %04Y zero-pads years like 216 to 0216 so they don't fail SQL ingestion
                        record[key] = (
                            None if dt.year < 2000 else format_date(dt, "%04Y-%m-%d")
                        )
                    except:
                        record[key] = None
            elif prop.get("format") == "date-time":
                if val == None:
                    record[key] = None
                else:
                    try:
                        if int(val) < 0:
                            record[key] = None
                        else:
                            record[key] = format_date(datetime.fromtimestamp(int(val)))
                    except:
                        record[key] = format_date(parse_date(val))
            elif "boolean" in prop.get("type"):
                record[key] = (
                    True if val == "TRUE" else False if val == "FALSE" else None
                )

    return record


def get_abs_path(p):
    return path.join(path.dirname(path.realpath(__file__)), p)


def transform_column_name(s):
    return s.strip().replace(" ", "_").lower().replace("/", "_or_")
