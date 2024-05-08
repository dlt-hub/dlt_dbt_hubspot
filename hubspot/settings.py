"""Hubspot source settings and constants"""
from dlt.common import pendulum

STARTDATE = pendulum.datetime(year=2024, month=2, day=10)

CRM_CONTACTS_ENDPOINT = (
    "/crm/v3/objects/contacts?associations=deals,products,tickets,quotes"
)
CRM_COMPANIES_ENDPOINT = (
    "/crm/v3/objects/companies?associations=contacts,deals,products,tickets,quotes"
)
CRM_DEALS_ENDPOINT = "/crm/v3/objects/deals"
CRM_PRODUCTS_ENDPOINT = "/crm/v3/objects/products"
CRM_TICKETS_ENDPOINT = "/crm/v3/objects/tickets"
CRM_QUOTES_ENDPOINT = "/crm/v3/objects/quotes"
CRM_OWNERS_ENDPOINT = "/crm/v3/owners/"
CRM_PROPERTIES_ENDPOINT = "/crm/v3/properties/{objectType}/{property_name}"
CRM_PIPELINES_ENDPOINT = "/crm/v3/pipelines/{objectType}"

CRM_OBJECT_ENDPOINTS = {
    "contact": CRM_CONTACTS_ENDPOINT,
    "company": CRM_COMPANIES_ENDPOINT,
    "deal": CRM_DEALS_ENDPOINT,
    "product": CRM_PRODUCTS_ENDPOINT,
    "ticket": CRM_TICKETS_ENDPOINT,
    "quote": CRM_QUOTES_ENDPOINT,
    "owner": CRM_OWNERS_ENDPOINT,
}

WEB_ANALYTICS_EVENTS_ENDPOINT = "/events/v3/events?objectType={objectType}&objectId={objectId}&occurredAfter={occurredAfter}&occurredBefore={occurredBefore}&sort=-occurredAt"

OBJECT_TYPE_SINGULAR = {
    "companies": "company",
    "contacts": "contact",
    "deals": "deal",
    "tickets": "ticket",
    "products": "product",
    "quotes": "quote",
    "owners": "owner",
}

OBJECT_TYPE_PLURAL = {v: k for k, v in OBJECT_TYPE_SINGULAR.items()}
ALL_OBJECTS = OBJECT_TYPE_PLURAL.keys()

DEFAULT_PROPERTIES = {
    "company": [
        "createdate",
        "domain",
        "hs_lastmodifieddate",
        "hs_object_id",
        "name",
    ],
    "contact": [
        "createdate",
        "email",
        "firstname",
        "hs_object_id",
        "lastmodifieddate",
        "lastname",
    ],
    "deal": [
        "amount",
        "closedate",
        "createdate",
        "dealname",
        "dealstage",
        "hs_lastmodifieddate",
        "hs_object_id",
        "pipeline",
    ],
    "ticket": [
        "createdate",
        "content",
        "hs_lastmodifieddate",
        "hs_object_id",
        "hs_pipeline",
        "hs_pipeline_stage",
        "hs_ticket_category",
        "hs_ticket_priority",
        "subject",
    ],
    "product": [
        "createdate",
        "description",
        "hs_lastmodifieddate",
        "hs_object_id",
        "name",
        "price",
    ],
    "quote": [
        "hs_createdate",
        "hs_expiration_date",
        "hs_lastmodifieddate",
        "hs_object_id",
        "hs_public_url_key",
        "hs_status",
        "hs_title",
    ],
}

ALL = [{"properties": "All"}]
PIPELINES_OBJECTS = ["deals"]
SOFT_DELETE_KEY = "is_deleted"
ARCHIVED_PARAM = {"archived": True}
PREPROCESSING = {"split": ["hs_merged_object_ids"]}
PROPERTIES_CONFIG_KEY = "sources.hubspot.{}.properties"
PROPERTY_LABEL_CONFIG_KEY = "sources.hubspot.property_label"
STAGE_PROPERTY_PREFIX = "hs_date_entered_"
MAX_PROPS_LENGTH = 2000
