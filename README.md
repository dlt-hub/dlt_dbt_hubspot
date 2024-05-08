# Hubspot source

This package contain dlt hubspot source and dbt package for this source.

## How to run

To load data from hubspot source you need:

1. Install requirements:

```
poetry install
```

2. Set correct credentials inside `.dlt/secrets.toml`:

```
[sources.hubspot]
api_key = "you-key"
```

3. Tune parameters inside `hubspot_pipeline.py`:

```
pipeline_name = "hubspot_pipeline"  # name of the pipeline
dataset_name_dlt = "hubspot"  # name of the dataset in dlt
dataset_name_dbt = "hubspot"  # name of the dataset in dbt
destination = "snowflake"  # name of the destination

# limit of extracted records for each resource
# (set this low for testing purposes or None if you want to load all data)
limit = 50
write_disposition = "merge"  # write disposition
```

4. Run the `hubspot_pipeline.py`:

```
python hubspot_pipeline.py
```

## How to configure tables

To configure names of the columns, properties and dlt resources you need to go to `.dlt/config.toml`.
And add the desired list of resources:

```toml
resources = [
    "contacts",
    "pipelines_deals",
    "owners",
    "contacts_property_history",
]
```

And add desired properties for each requested resource.
```toml
[[sources.hubspot.deals.properties]]
name = "estimated_kick_off_date"  # name of the property
alias = "kick_off_date"  # corresponding alias for dbt
add_property_label = true  # whether to change the property values to labels
```
