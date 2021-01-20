from tap_costcon.fetch import handle_generic

ID_FIELDS = {"job_details": ["job_number"], "debtor_transactions": ["index"]}

HANDLERS = {
    "job_details": handle_generic(
        mappings={
            "Job": "job_number",
            "Date2 ProjectOpen": "date_project_open",
            "SiteManager": "site_manager",
            "QuantitySurveyorCode": "quantity_surveyor_code",
            "Date6 PracticalCompletion": "date_practical_completion",
        },
        unique_key="job_number",
    ),
    "debtor_transactions": handle_generic(
        use_index=True, trim_columns=["transaction_description"]
    ),
}
