from tap_costcon.fetch import handle_generic, transform_job_details, transform_gl_lines

ID_FIELDS = {
    "categories": ["code"],
    "contacts": ["contact_code"],
    "cost_transactions": ["ct_guid"],
    "creditor_transactions": ["ct_guid"],
    "debtor_transactions": ["ct_guid"],
    "debtor_transaction_lines": ["ct_guid"],
    "gl_codes": ["ct_guid"],
    "gl_lines": ["ct_guid"],
    "job_costs_inquiry": ["ct_guid"],
    "job_costs_summary_inquiry": ["id"],
    "job_details": ["job_number"],
    "job_subcontractors": ["ct_guid"],
    "subcategories": ["code"],
    "variation_orders": ["ct_guid"],
}

HANDLERS = {
    "categories": handle_generic(unique_key="code"),
    "contacts": handle_generic(unique_key="contact_code"),
    "cost_transactions": handle_generic(),
    "creditor_transactions": handle_generic(),
    "debtor_transactions": handle_generic(
        trim_columns=["transaction_description"],
        # date_modified is mostly null, and rows can't be edited once posted
    ),
    "debtor_transaction_lines": handle_generic(trim_columns=["line_description"]),
    "gl_codes": handle_generic(),
    "gl_lines": handle_generic(transform_fn=transform_gl_lines),
    "job_costs_inquiry": handle_generic(
        mappings={
            "Job": "job_number",
            "VO#": "variation_order_number",
            "Job Variation Order GUID": "variation_order_guid",
        },
    ),
    "job_costs_summary_inquiry": handle_generic(
        mappings={
            "Job": "job_number",
            "VOs (Internal)": "internal_variations",
            "Costs to Date (inc unposted)": "costs_to_date",
        },
        id_function=lambda row: row["job_number"] + "_" + row["subcategory"],
        # works fine locally without explicitly specifying this but breaks in prod
        date_column=None,
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
        transform_fn=transform_job_details,
    ),
    "job_subcontractors": handle_generic(),
    "subcategories": handle_generic(unique_key="code"),
    "variation_orders": handle_generic(),
}
