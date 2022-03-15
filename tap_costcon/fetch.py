import re
import singer
import singer.metrics as metrics
from singer import metadata
from singer.bookmarks import get_bookmark
from datetime import datetime
from tap_costcon.utility import (
    list_files,
    parse_date,
    format_date,
    get_time,
    construct_date,
    parse_csv,
    transform_record,
)

job_regex = re.compile(r"^\d+")


def handle_generic(
    mappings=None,
    id_function=None,
    unique_key="ct_guid",
    trim_columns=[],
    date_column="ct_modified_timestamp",
    date_column_type="timestamp",
    transform_fn=None,
):
    unique_key_name = "id" if id_function is not None else unique_key

    def do_generic(resource, schema, state, mdata, folder_path):
        extraction_time = get_time()
        bookmark = get_bookmark(state, resource, "since")
        parsed_bookmark = (
            None
            if bookmark is None or date_column is None
            else parse_date(bookmark)
            if date_column_type == "timestamp"
            else datetime.date(parse_date(bookmark))
        )
        properties = schema["properties"]

        files = list_files(folder_path + resource)
        to_sync = [
            (file, time)
            for (file, time) in files
            if bookmark == None or datetime.fromtimestamp(time) > parse_date(bookmark)
        ]

        # many duplicate records; way faster to deduplicate in memory than to send to Redshift
        # small performance hit for a batch with one file but massive performance improvements otherwise
        unique = {}

        for (file, modified_time) in to_sync:
            if mappings is not None:
                records = parse_csv(file, mappings=mappings)
            else:
                records = parse_csv(file)

            for record in records:
                record["file_modified_time"] = modified_time
                row = transform_record(properties, record, trim_columns=trim_columns)

                # row-level filtering where possible
                parsed_date_column = (
                    None
                    if bookmark is None
                    or date_column is None
                    or row[date_column] is None
                    else parse_date(row[date_column])
                    if date_column_type == "timestamp"
                    else datetime.date(parse_date(row[date_column], "%Y-%m-%d"))
                )
                if (
                    parsed_date_column is not None
                    and parsed_date_column < parsed_bookmark
                ):
                    continue

                if transform_fn is not None:
                    row = transform_fn(row)

                if id_function is not None:
                    row["id"] = id_function(row)

                # the primary key should never be blank
                if row[unique_key_name] is None or row[unique_key_name] == "":
                    continue

                unique[row[unique_key_name]] = row

        with metrics.record_counter(resource) as counter:
            for row in unique.values():
                write_record(row, resource, schema, mdata, extraction_time)
                counter.increment()
        return write_bookmark(state, resource, extraction_time)

    return do_generic


def transform_job_details(row):
    mapped = row["job_number"]

    # Redshift doesn't have an easy try_cast function so can't do the try_except block below
    # for specific conditions, see notes at https://www.notion.so/fosters/Board-reporting-f5d403ed64c44594873e9fcffec9a3ef#46b4428950834c7bafb94eb0fc22e9e9
    # note that about 20 rows have null `ct_created_timestamp`, which is a data source error in Costcon. None of these are old jobs so can safely keep the original job_number
    if row["ct_created_timestamp"]:
        year = int(row["ct_created_timestamp"][:4])
        if row["company_code"] == "FOSTER" and year >= 2005:
            reg = job_regex.search(mapped)
            if reg:
                mapped = int(reg.group(0))

    row["consolidated_job_number"] = mapped
    return row


def transform_gl_lines(row):
    # reconstruct date from financial year and period, because it's infrequently different and Costcon reporting is based on financial year and period
    fy = int(row["year"])
    period = int(row["period"])
    d = int(row["transaction_date"][-2:]) if row["transaction_date"] is not None else 1
    year = fy if period > 9 else fy - 1
    month = ((period + 2) % 12) + 1
    try:
        dt = construct_date(year, month, d)
    except:
        # if the original transaction_date was on the 31st and the new month is say April, then constructing the date will throw
        # this is rare and there's no good answer, so the 1st of the month will work fine
        dt = construct_date(year, month, 1)
    row["transaction_date"] = dt

    return row


# More convenient to use but has to all be held in memory, so use write_record instead for resources with many rows
def write_many(rows, resource, schema, mdata, dt):
    with metrics.record_counter(resource) as counter:
        for row in rows:
            write_record(row, resource, schema, mdata, dt)
            counter.increment()


def write_record(row, resource, schema, mdata, dt):
    with singer.Transformer() as transformer:
        rec = transformer.transform(row, schema, metadata=metadata.to_map(mdata))
    singer.write_record(resource, rec, time_extracted=dt)


def write_bookmark(state, resource, dt):
    singer.write_bookmark(state, resource, "since", format_date(dt))
    return state
