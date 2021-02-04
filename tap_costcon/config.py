from tap_costcon.fetch import handle_generic

ID_FIELDS = {
    "cost_transactions": ["ct_guid"],
    "debtor_transactions": ["index"],
    "job_details": ["job_number"],
}

HANDLERS = {
    "cost_transactions": handle_generic(
        unique_key="ct_guid", date_column="ct_modified_timestamp"
    ),
    "debtor_transactions": handle_generic(
        use_index=True,
        trim_columns=["transaction_description"],
        # date_modified is mostly null, and rows can't be edited once posted
        date_column="date_posted",
        date_column_type="date",
    ),
    "job_details": handle_generic(
        mappings={
            "Job": "job_number",
            "Date2 ProjectOpen": "date_project_open",
            "SiteManager": "site_manager",
            "QuantitySurveyorCode": "quantity_surveyor_code",
            "Date6 PracticalCompletion": "date_practical_completion",
        },
        unique_key="job_number",
        date_column="date_modified",
        date_column_type="date",
    ),
}
