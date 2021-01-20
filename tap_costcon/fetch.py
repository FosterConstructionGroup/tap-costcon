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
    hash,
    parse_csv,
    transform_record,
)


def handle_generic(
    mappings=None, use_index=False, id_function=None, unique_key=None, trim_columns=[]
):
    unique_key_name = (
        "index" if use_index else "id" if id_function is not None else unique_key
    )

    def do_generic(resource, schema, state, mdata, folder_path):
        extraction_time = get_time()
        bookmark = get_bookmark(state, resource, "since")
        properties = schema["properties"]

        files = list_files(folder_path + resource)
        to_sync = (
            [file for (file, time) in files]
            if bookmark == None
            else [
                file
                for (file, time) in files
                if datetime.fromtimestamp(time) > parse_date(bookmark)
            ]
        )

        # many duplicate records; way faster to deduplicate in memory than to send to Redshift
        # small performance hit for a batch with one file but massive performance improvements otherwise
        unique = {}

        for file in to_sync:
            if mappings is not None:
                records = parse_csv(file, mappings=mappings)
            else:
                records = parse_csv(file)

            # initialise index in case it's used in future
            # needs to reset for every new file, as it's acting as a proxy ID
            # could conditionally initialise, but linter warns about potentially unbound variable so this fixes that warning
            index = 0

            for record in records:
                row = transform_record(properties, record, trim_columns=trim_columns)

                if use_index:
                    index += 1
                    row["index"] = index
                elif id_function is not None:
                    row["id"] = id_function(row)

                unique[row[unique_key_name]] = row

        with metrics.record_counter(resource) as counter:
            for row in unique.values():
                write_record(row, resource, schema, mdata, extraction_time)
                counter.increment()
        return write_bookmark(state, resource, extraction_time)

    return do_generic


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
