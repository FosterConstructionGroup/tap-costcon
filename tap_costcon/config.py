from tap_costcon.fetch import handle_jobs

ID_FIELDS = {
    "jobs": ["job_number"],
}

HANDLERS = {
    "jobs": handle_jobs,
}

