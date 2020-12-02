from tap_costcon.fetch import handle_job_details

ID_FIELDS = {
    "job_details": ["job_number"],
}

HANDLERS = {
    "job_details": handle_job_details,
}

