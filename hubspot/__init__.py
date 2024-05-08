"""
This is a module that provides a DLT source to retrieve data from multiple endpoints of the HubSpot API
using a specified API key. The retrieved data is returned as a tuple of Dlt resources, one for each endpoint.

The source retrieves data from the following endpoints:
- CRM Companies
- CRM Contacts
- CRM Deals
- CRM Tickets
- CRM Products
- CRM Quotes
- CRM Owners
- CRM Pipelines
- Web Analytics Events

For each endpoint, a resource and transformer function are defined to retrieve data and transform it to a common format.
The resource functions yield the raw data retrieved from the API, while the transformer functions are used to retrieve
additional information from the Web Analytics Events endpoint.

The source also supports enabling Web Analytics Events for each endpoint by setting the corresponding enable flag to True.

Example:
To retrieve data from all endpoints, use the following code:

python

>>> resources = hubspot(api_key="your_api_key")
"""

from typing import Any, Dict, Iterator, List, Literal, Sequence
from urllib.parse import quote

import dlt
from dlt.common import pendulum
from dlt.common.typing import TDataItems
from dlt.sources import DltResource

from .helpers import (
    _get_property_names,
    fetch_data,
    fetch_property_history,
    get_properties_labels,
)
from .settings import (
    ALL,
    ALL_OBJECTS,
    ARCHIVED_PARAM,
    CRM_OBJECT_ENDPOINTS,
    CRM_PIPELINES_ENDPOINT,
    DEFAULT_PROPERTIES,
    MAX_PROPS_LENGTH,
    OBJECT_TYPE_PLURAL,
    OBJECT_TYPE_SINGULAR,
    PIPELINES_OBJECTS,
    PROPERTIES_CONFIG_KEY,
    SOFT_DELETE_KEY,
    STAGE_PROPERTY_PREFIX,
    STARTDATE,
    WEB_ANALYTICS_EVENTS_ENDPOINT,
)

THubspotObjectType = Literal["company", "contact", "deal", "ticket", "product", "quote"]


def extract_properties_list(props: Dict[str, Any]):
    """Flatten the properties dict to list"""
    return [prop.get("name") for prop in props]


def fetch_data_for_properties(props, api_key, object_type, soft_delete):
    params = {"properties": props, "limit": 100}
    context = {SOFT_DELETE_KEY: False} if soft_delete else None

    yield from fetch_data(
        CRM_OBJECT_ENDPOINTS[object_type], api_key, params=params, context=context
    )
    if soft_delete:
        yield from fetch_data(
            CRM_OBJECT_ENDPOINTS[object_type],
            api_key,
            params={**params, **ARCHIVED_PARAM},
            context={SOFT_DELETE_KEY: True},
        )


def crm_objects(
    object_type: str,
    api_key: str = dlt.secrets.value,
    props: Sequence[str] = None,
    include_custom_props: bool = True,
    archived: bool = False,
) -> Iterator[TDataItems]:
    """Building blocks for CRM resources."""
    props = fetch_props(object_type, api_key, props, include_custom_props)
    yield from fetch_data_for_properties(props, api_key, object_type, archived)


def crm_object_history(
    object_type: THubspotObjectType,
    api_key: str = dlt.secrets.value,
    include_custom_props: bool = True,
):
    """Returns generator which iterating over properties history."""
    props = (
        dlt.config.get(PROPERTIES_CONFIG_KEY.format(OBJECT_TYPE_PLURAL[object_type]))
        or DEFAULT_PROPERTIES[object_type]
    )
    props = fetch_props(object_type, api_key, props, include_custom_props)
    yield from fetch_property_history(
        CRM_OBJECT_ENDPOINTS[object_type],
        api_key,
        props,
    )


def resource_template(
    entity: THubspotObjectType,
    api_key: str = dlt.config.value,
    properties: Dict[str, Any] = dlt.config.value,
    include_custom_props: bool = False,
    soft_delete: bool = False,
):
    """This is function yield specific resource records."""
    yield from crm_objects(
        entity,
        api_key,
        props=properties,
        include_custom_props=include_custom_props,
        archived=soft_delete,
    )


def resource_history_template(
    entity: THubspotObjectType,
    api_key: str = dlt.config.value,
    include_custom_props: bool = False,
):
    """This is function yield specific resource properties history."""
    yield from crm_object_history(
        entity, api_key, include_custom_props=include_custom_props
    )


@dlt.resource(name="properties", write_disposition="replace")
def hubspot_properties(
    properties_list: List[Dict[str, Any]] = None,
    api_key: str = dlt.secrets.value,
) -> DltResource:
    """
    A standalone DLT resources that retrieves properties.

    Args:
        properties_list(List[THubspotObjectType], required): List of the hubspot object types see definition of THubspotObjectType literal.
        api_key (str, optional): The API key used to authenticate with the HubSpot API. Defaults to dlt.secrets.value.

    Returns:
        Incremental dlt resource to track properties for objects from the list
    """

    def get_properties_with_labels_from_config():
        """Extract properties for which we need to fetch labels."""
        properties_list = []
        for object_type in ALL_OBJECTS:
            object_cols = (
                dlt.config.get(
                    PROPERTIES_CONFIG_KEY.format(OBJECT_TYPE_PLURAL[object_type])
                )
                or []
            )
            for col in object_cols:
                if col.get("add_property_label", False):
                    properties_list.append(
                        {"object_type": object_type, "property_name": col["name"]}
                    )
        return properties_list

    def get_properties_description(properties_list):
        """Fetch properties."""
        for property_info in properties_list:
            yield get_properties_labels(
                api_key=api_key,
                object_type=property_info["object_type"],
                property_name=property_info["property_name"],
            )

    properties_list = properties_list or get_properties_with_labels_from_config()
    yield from get_properties_description(properties_list)


def pivot_stages_properties(data, property_prefix=STAGE_PROPERTY_PREFIX, id_prop="id"):
    new_data = []
    for record in data:
        record_not_null = {k: v for k, v in record.items() if v is not None}
        if id_prop not in record_not_null:
            continue
        id_val = record_not_null.pop(id_prop)
        new_data += [
            {id_prop: id_val, property_prefix: v, "stage": k.split(property_prefix)[1]}
            for k, v in record_not_null.items()
            if k.startswith(property_prefix)
        ]
    return new_data


def stages_timing(
    object_type: str, api_key: str = dlt.config.value, soft_delete: bool = False
):
    all_properties = list(_get_property_names(api_key, object_type))
    date_entered_properties = [
        prop for prop in all_properties if prop.startswith(STAGE_PROPERTY_PREFIX)
    ]
    props = ",".join(date_entered_properties)
    idx = 0
    while idx < len(props):
        if len(props) - idx < MAX_PROPS_LENGTH:
            props_part = ",".join(props[idx: idx + MAX_PROPS_LENGTH].split(",")[:-1])
        else:
            props_part = props[idx: idx + MAX_PROPS_LENGTH]
        idx += len(props_part)
        for data in fetch_data_for_properties(
            props_part, api_key, object_type, soft_delete
        ):
            yield pivot_stages_properties(data)


@dlt.source(name="hubspot")
def hubspot(
    api_key: str = dlt.secrets.value,
    include_history: bool = False,
    soft_delete: bool = False,
    include_custom_props: bool = True,
) -> Sequence[DltResource]:
    """
    A DLT source that retrieves data from the HubSpot API using the
    specified API key.

    This function retrieves data for several HubSpot API endpoints,
    including companies, contacts, deals, tickets, products and web
    analytics events. It returns a tuple of Dlt resources, one for
    each endpoint.

    Args:
        api_key (Optional[str]):
            The API key used to authenticate with the HubSpot API. Defaults
            to dlt.secrets.value.
        include_history (Optional[bool]):
            Whether to load history of property changes along with entities.
            The history entries are loaded to separate tables.
        soft_delete (bool):
            Whether to fetch deleted properties and mark them as `is_deleted`.
        include_custom_props (bool):
            Whether to include custom properties.

    Returns:
        Sequence[DltResource]: Dlt resources, one for each HubSpot API endpoint.

    Notes:
        This function uses the `fetch_data` function to retrieve data from the
        HubSpot CRM API. The API key is passed to `fetch_data` as the
        `api_key` argument.
    """

    @dlt.resource(name="owners", write_disposition="merge", primary_key="id")
    def owners(
        api_key: str = api_key,
    ) -> Iterator[TDataItems]:
        """Hubspot owner resource"""
        for page in fetch_data(endpoint=CRM_OBJECT_ENDPOINTS["owner"], api_key=api_key):
            yield page
        if soft_delete:
            for page in fetch_data(
                endpoint=CRM_OBJECT_ENDPOINTS["owner"],
                params=ARCHIVED_PARAM,
                api_key=api_key,
                context={SOFT_DELETE_KEY: True},
            ):
                yield page

    def hubspot_pipelines_for_objects(
        api_key: str = dlt.secrets.value,
    ) -> DltResource:
        """
        A standalone DLT resources that retrieves properties.

        Args:
            object_type(List[THubspotObjectType], required): List of the hubspot object types see definition of THubspotObjectType literal.
            api_key (str, optional): The API key used to authenticate with the HubSpot API. Defaults to dlt.secrets.value.

        Returns:
            Incremental dlt resource to track properties for objects from the list
        """

        def get_pipelines(object_type: THubspotObjectType):
            yield from fetch_data(
                CRM_PIPELINES_ENDPOINT.format(objectType=object_type),
                api_key=api_key,
            )

        for obj_type in PIPELINES_OBJECTS:
            name = f"pipelines_{obj_type}"
            yield dlt.resource(
                get_pipelines,
                name=name,
                write_disposition="merge",
                merge_key="id",
                table_name=name,
                primary_key="id",
            )(obj_type)

            name = f"stages_timing_{obj_type}"
            if obj_type in OBJECT_TYPE_SINGULAR:
                yield dlt.resource(
                    stages_timing,
                    name=name,
                    write_disposition="merge",
                    primary_key=["id", "stage"],
                )(OBJECT_TYPE_SINGULAR[obj_type], soft_delete=soft_delete)

    for obj in ALL_OBJECTS:
        yield dlt.resource(
            resource_template,
            name=OBJECT_TYPE_PLURAL[obj],
            write_disposition="merge",
            primary_key="id",
        )(
            entity=obj,
            include_custom_props=include_custom_props,
            soft_delete=soft_delete,
        )

    if include_history:
        for obj in ALL_OBJECTS:
            yield dlt.resource(
                resource_history_template,
                name=f"{OBJECT_TYPE_PLURAL[obj]}_property_history",
                write_disposition="merge",
                primary_key="object_id",
            )(entity=obj, include_custom_props=include_custom_props)

    yield from hubspot_pipelines_for_objects(api_key)
    yield hubspot_properties


def fetch_props(
    object_type: str,
    api_key: str,
    props: Dict[str, Any] = None,
    include_custom_props: bool = True,
):
    if props == ALL:
        props = list(_get_property_names(api_key, object_type))
    else:
        props = extract_properties_list(props)

    if include_custom_props:
        all_props = _get_property_names(api_key, object_type)
        custom_props = [prop for prop in all_props if not prop.startswith("hs_")]
        props = props + custom_props  # type: ignore

    props = ",".join(sorted(list(set(props))))

    if len(props) > MAX_PROPS_LENGTH:
        raise ValueError(
            "Your request to Hubspot is too long to process. "
            f"Maximum allowed query length is {MAX_PROPS_LENGTH} symbols, while "
            f"your list of properties `{props[:200]}`... is {len(props)} "
            "symbols long. Use the `props` argument of the resource to "
            "set the list of properties to extract from the endpoint."
        )
    return props


@dlt.resource
def hubspot_events_for_objects(
    object_type: THubspotObjectType,
    object_ids: List[str],
    api_key: str = dlt.secrets.value,
    start_date: pendulum.DateTime = STARTDATE,
) -> DltResource:
    """
    A standalone DLT resources that retrieves web analytics events from the HubSpot API for a particular object type and list of object ids.

    Args:
        object_type(THubspotObjectType, required): One of the hubspot object types see definition of THubspotObjectType literal
        object_ids: (List[THubspotObjectType], required): List of object ids to track events
        api_key (str, optional): The API key used to authenticate with the HubSpot API. Defaults to dlt.secrets.value.
        start_date (datetime, optional): The initial date time from which start getting events, default to STARTDATE

    Returns:
        incremental dlt resource to track events for objects from the list
    """

    end_date = pendulum.now().isoformat()
    name = object_type + "_events"

    def get_web_analytics_events(
        occurred_at: dlt.sources.incremental[str],
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        A helper function that retrieves web analytics events for a given object type from the HubSpot API.

        Args:
            object_type (str): The type of object for which to retrieve web analytics events.

        Yields:
            dict: A dictionary representing a web analytics event.
        """
        for object_id in object_ids:
            yield from fetch_data(
                WEB_ANALYTICS_EVENTS_ENDPOINT.format(
                    objectType=object_type,
                    objectId=object_id,
                    occurredAfter=quote(occurred_at.last_value),
                    occurredBefore=quote(end_date),
                ),
                api_key=api_key,
            )

    return dlt.resource(
        get_web_analytics_events,
        name=name,
        primary_key="id",
        write_disposition="append",
        selected=True,
        table_name=lambda e: name + "_" + str(e["eventType"]),
    )(dlt.sources.incremental("occurredAt", initial_value=start_date.isoformat()))
