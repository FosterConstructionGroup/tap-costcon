import singer
import singer.metrics as metrics
from singer import metadata
from singer.bookmarks import get_bookmark
from datetime import datetime
from tap_costcon.utility import (
    list_files,
    parse_date,
    format_date,
    hash,
    parse_csv,
    transform_record,
)


def handle_jobs(resource, schema, state, mdata, folder_path):
    extraction_time = singer.utils.now()
    bookmark = get_bookmark(state, resource, "since")
    properties = schema["properties"]

    files = list_files(folder_path, resource)
    to_sync = (
        files
        if bookmark == None
        else [
            file
            for (file, time) in files
            if datetime.fromtimestamp(time) > parse_date(bookmark)
        ]
    )

    with metrics.record_counter(resource) as counter:
        for file in to_sync:
            mappings = {
                "Job": "job_number",
                "Date2 ProjectOpen": "date_project_open",
                "SiteManager": "site_manager",
                "QuantitySurveyorCode": "quantity_surveyor_code",
                "Date6 PracticalCompletion": "date_practical_completion",
            }

            records = parse_csv(file, mappings=mappings)

            for record in records:
                row = transform_record(properties, record)
                write_record(row, resource, schema, mdata, extraction_time)
                counter.increment()
    return write_bookmark(state, resource, extraction_time)


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
