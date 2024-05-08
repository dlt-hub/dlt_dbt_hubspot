from typing import Optional

import dlt

from hubspot import hubspot, settings
from hubspot.utils import split_data


def get_resources():
    """Function which retrieve list of resources from config."""
    return dlt.config.get("sources.hubspot.resources").get("resources")


def get_env_variables_from_config():
    """Function which retrieve all properties for all resources from config."""
    vars = {}
    for resource in get_resources():
        vars[resource] = dlt.config.get(settings.PROPERTIES_CONFIG_KEY.format(resource))
    return vars


def get_source():
    load_data = hubspot(
        soft_delete=True, include_history=True, include_custom_props=False
    ).with_resources(*get_resources())
    load_data.deals.add_map(split_data, 1)
    return load_data


def load_crm_data(
    pipeline_name: str,
    dataset_name: str,
    destination: str,
    limit: Optional[int] = None,
    write_disposition: str = "replace",
) -> None:
    """This function loads all resources from HubSpot CRM."""

    # Create a DLT pipeline object with the pipeline name, dataset name, and destination database type
    # Add full_refresh=(True or False) if you need your pipeline to create the dataset in your destination
    p = dlt.pipeline(
        pipeline_name=pipeline_name,
        dataset_name=dataset_name,
        destination=destination,
        progress="log",
    )
    load_data = get_source()
    if limit:
        load_data = load_data.add_limit(limit)

    # Run the pipeline with the HubSpot source connector
    info = p.run(load_data, write_disposition=write_disposition)
    print(info)


def run_dbt_package(
    pipeline_name: str, dataset_name_dlt: str, dataset_name_dbt: str, destination: str
):
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name_dlt,
        progress="log",
    )

    dbt = dlt.dbt.package(pipeline, "dbt_hubspot")

    additional_vars = get_env_variables_from_config()
    additional_vars.update(
        {
            "schema": dataset_name_dlt,
            "property_label": settings.PROPERTY_LABEL_CONFIG_KEY,
        }
    )
    models = dbt.run_all(
        additional_vars=additional_vars,
        destination_dataset_name=dataset_name_dbt,
    )

    # on success print outcome
    for m in models:
        print(
            f"Model {m.model_name} materialized "
            + f"in {m.time} "
            + f"with status {m.status} "
            + f"and message {m.message}"
        )


if __name__ == "__main__":
    # Call the functions to load HubSpot data into the database with and without company events enabled
    pipeline_name = "hubspot_pipeline"  # name of the pipeline
    dataset_name_dlt = "hubspot"  # name of the dataset in dlt
    dataset_name_dbt = "hubspot"  # name of the dataset in dbt
    destination = "snowflake"  # name of the destination

    # limit of extracted records for each resource
    # (set this low for testing purposes or None if you want to load all data)
    limit = 50
    write_disposition = "merge"  # write disposition

    load_crm_data(
        pipeline_name,
        dataset_name_dlt,
        destination,
        limit=limit,
        write_disposition=write_disposition,
    )
    run_dbt_package(pipeline_name, dataset_name_dlt, dataset_name_dbt, destination)
