from tap_costcon.fetch import handle_generic

ID_FIELDS = {
    "contacts": ["contact_code"],
    "cost_transactions": ["ct_guid"],
    "debtor_transactions": ["ct_guid"],
    "job_costs_inquiry": ["ct_guid"],
    "job_details": ["job_number"],
}

HANDLERS = {
    "contacts": handle_generic(
        unique_key="contact_code", date_column="ct_modified_timestamp"
    ),
    "cost_transactions": handle_generic(
        unique_key="ct_guid", date_column="ct_modified_timestamp"
    ),
    "debtor_transactions": handle_generic(
        unique_key="ct_guid",
        trim_columns=["transaction_description"],
        # date_modified is mostly null, and rows can't be edited once posted
        date_column="ct_modified_timestamp",
    ),
    "job_costs_inquiry": handle_generic(
        unique_key="ct_guid",
        mappings={
            "Job": "job_number",
            "VO#": "variation_order_number",
            "Job Variation Order GUID": "variation_order_guid",
        },
        date_column="ct_modified_timestamp",
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
        date_column="ct_modified_timestamp",
    ),
}
