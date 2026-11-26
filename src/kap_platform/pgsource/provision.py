"""KAP objects for a dataset: S3 connector, pipeline, collection.

Created once per dataset and reused by name (idempotent): if an object with
the expected name already exists in the tenant it is used as is, never
patched (patching a pipeline makes KAP reprocess every collection using it).

- indicator presets: indicators pipeline (csv_loader), collection without entities;
- `records`: document pipeline (docx parser → summary transformer → embedding) and a
  collection whose entity grouping is "folder at depth 1" — one record per folder, so
  `hybrid_search` returns one summary per record (setup verified on the stand, Nov 2026).

Connector keys are sent as plain strings: the stand runs KAP with
SECRET_PROVIDER_MODE=disabled. With a secrets store they would have to be
`{"$secret": …}` references instead.
"""
from __future__ import annotations

import copy
from datetime import datetime, timezone
from typing import Any, Mapping

from ..config import Settings
from ..kap.client import KapClient
from .kapcsv import FILES
from .s3 import object_prefix

SYNC_INTERVAL = "PT1M"


class ProvisionError(Exception):
    """An existing KAP object with the expected name cannot be used for this dataset."""


# The sandbox indicators pipeline (sandbox_indicators_pipeline on the stand), verbatim but for name/model.
PIPELINE_TEMPLATE: dict[str, Any] = {
    "kind": "indicators",
    "mode": "snapshot",
    "embedding_model_ref": "embed_default",
    "stages": [
        {
            "name": "load_csv",
            "type": "csv_loader",
            "config": {
                "files": {"**/registry.csv": "registry", "**/observations.csv": "observations", "**/narratives.csv": "narratives"},
                "encoding": "utf-8-sig",
                "delimiter": ",",
                "decimal_separator": ".",
                "date_formats": ["%Y-%m-%d"],
                "period_formats": ["%Y-%m", "%Y-Q%q", "%Y"],
                "null_values": ["", "null", "NULL"],
                "validation": {},
            },
        }
    ],
}

# records preset — prompts that kept every fact on the stand (same defaults as kap-console data/search.ts)
DOC_SUMMARY_PROMPT = (
    "Summarise this document in Markdown. Start with its reference and title. Keep every fact exactly as written: "
    "all names, roles, dates, figures, units, table rows (including empty or \"not yet reported\" values), decisions, "
    "risks and any control phrase. Do not shorten lists. Do not add facts, calculations or comparisons."
)
GROUP_SUMMARY_PROMPT = (
    "Combine the document summaries into one Markdown text with one section per document, headed by its reference and title. "
    "Never drop a document and never merge two documents into one section. Keep every fact exactly as written: names, roles, "
    "dates, figures, units, table rows, decisions, risks and control phrases. When documents disagree, keep both claims and "
    "name their documents. Do not add facts, calculations or comparisons."
)
PREFERRED_ENTITY_TYPE = "indicator_document_context"
FALLBACK_ENTITY_TYPE: dict[str, Any] = {
    "name": "document_group",
    "description": "Generic document grouping for agent search",
    "schema": {"type": "object", "properties": {}},
}
GROUP_TEMPLATE = "{collection_id}:{relative_folder_path}"
CHUNK_SIZE, CHUNK_OVERLAP = 800, 120


def names(collection_name: str) -> dict[str, str]:
    return {"connector": f"{collection_name}_pg", "pipeline": f"{collection_name}_pipeline", "collection": collection_name}


def is_records(dataset: Mapping[str, Any]) -> bool:
    return dataset.get("preset") == "records"


def connector_body(name: str, tenant_id: str, dataset_name: str, settings: Settings, records: bool = False) -> dict[str, Any]:
    endpoint = (settings.s3_kap_url or settings.s3_url).rstrip("/")
    tls = endpoint.lower().startswith("https://")
    globs = ["**/*.docx"] if records else [f"{n}.csv" for n in FILES]
    return {
        "name": name,
        "type": "s3",
        "settings": {
            "object_selector": {"prefix": object_prefix(tenant_id, dataset_name), "include_globs": globs, "exclude_globs": []},
            "sync": {"interval": SYNC_INTERVAL},
        },
        "config": {
            "bucket_name": settings.s3_bucket,
            "endpoint_url": endpoint,
            "use_ssl": tls,
            "verify_tls": tls,
            "auth": {"type": "static_keys", "access_key": settings.s3_access_key, "secret_key": settings.s3_secret_key},
        },
    }


def pipeline_body(name: str, embedding_model_ref: str = "embed_default") -> dict[str, Any]:
    body = copy.deepcopy(PIPELINE_TEMPLATE)
    body["embedding_model_ref"] = embedding_model_ref
    return {"name": name, **body}


def _prompts(p: str) -> dict[str, str]:
    return {"initial_prompt": p, "refine_prompt": p, "merge_prompt": p}


def document_pipeline_body(name: str, chat_model_ref: str = "chat_default", embedding_model_ref: str = "embed_default") -> dict[str, Any]:
    """parse (docx only) → summarize_document (summary task) → embed."""
    return {
        "name": name,
        "kind": "document",
        "stages": [
            {
                "name": "parse",
                "type": "parser",
                "routes": [{
                    "match": {"extensions": ["docx"]},
                    "parser": {"provider": "docx", "output": "markdown", "config": {"preserve_headings": True, "preserve_lists": True, "preserve_tables": True}},
                }],
            },
            {
                "name": "summarize_document",
                "type": "transformer",
                "config": {"model_ref": chat_model_ref, "tasks": [{"name": "summary", "enabled": True, "config": _prompts(DOC_SUMMARY_PROMPT)}]},
            },
            {
                "name": "embed",
                "type": "embedding",
                "config": {"model_ref": embedding_model_ref, "chunking": {"chunk_size": CHUNK_SIZE, "overlap": CHUNK_OVERLAP}},
            },
        ],
    }


def collection_body(name: str, pipeline: str, connector: str) -> dict[str, Any]:
    return {"name": name, "pipeline_ref": pipeline, "connectors": [connector], "sync_strategy": "overwrite", "entity": {"enabled": False}}


def entity_block(entity_type: str, chat_model_ref: str = "chat_default") -> dict[str, Any]:
    """One document group per folder at depth 1 below the connector prefix = one per record."""
    return {
        "enabled": True,
        "entity_type_ref": entity_type,
        "summary": {"model_ref": chat_model_ref, **_prompts(GROUP_SUMMARY_PROMPT)},
        "boundary": {"strategy": "folder", "collection_root": "/", "folder_depth": 1, "recursive": False},
        "identity": {"entity_id_template": GROUP_TEMPLATE},
        "metadata": None,
    }


def records_collection_body(name: str, pipeline: str, connector: str, entity_type: str, chat_model_ref: str = "chat_default") -> dict[str, Any]:
    return {**collection_body(name, pipeline, connector), "entity": entity_block(entity_type, chat_model_ref)}


async def find_by_name(kap: KapClient, tenant_id: str, resource: str, name: str) -> dict[str, Any] | None:
    async for item in kap.paginate(tenant_id, f"/api/{resource}"):
        if item.get("name") == name:
            return item
    return None


async def ensure_entity_type(kap: KapClient, tenant_id: str) -> str:
    """The generic grouping type of the tenant (indicator_document_context), else document_group (created if absent)."""
    found: set[str] = set()
    async for item in kap.paginate(tenant_id, "/api/entity-types"):
        found.add(str(item.get("name")))
    for name in (PREFERRED_ENTITY_TYPE, FALLBACK_ENTITY_TYPE["name"]):
        if name in found:
            return name
    created = await kap.request(tenant_id, "POST", "/api/entity-types", json=copy.deepcopy(FALLBACK_ENTITY_TYPE))
    return (created or {}).get("name") or FALLBACK_ENTITY_TYPE["name"]


async def ensure(kap: KapClient, tenant_id: str, dataset: Mapping[str, Any], settings: Settings) -> dict[str, Any]:
    """Make sure the dataset's connector, pipeline and collection exist. Returns the dataset's `kap` record."""
    records = is_records(dataset)
    kind = "document" if records else "indicators"
    want = names(dataset["collection_name"])
    current = dataset.get("kap") or {}
    if current.get("collection_id") and current.get("pipeline_id") and all(current.get(k) == v for k, v in want.items()) and current.get("kind", "indicators") == kind:
        return dict(current)

    connector = await find_by_name(kap, tenant_id, "connectors", want["connector"])
    if connector is None:
        connector = await kap.request(tenant_id, "POST", "/api/connectors", json=connector_body(want["connector"], tenant_id, dataset["name"], settings, records))
    pipeline = await find_by_name(kap, tenant_id, "pipelines", want["pipeline"])
    if pipeline is None:
        body = (document_pipeline_body(want["pipeline"], settings.kap_chat_model_ref, settings.kap_embedding_model_ref) if records
                else pipeline_body(want["pipeline"], settings.kap_embedding_model_ref))
        pipeline = await kap.request(tenant_id, "POST", "/api/pipelines", json=body)
    elif pipeline.get("kind") and pipeline["kind"] != kind:
        raise ProvisionError(
            f"KAP already has a pipeline {want['pipeline']} of kind {pipeline['kind']}; a {dataset.get('preset')} dataset needs a {kind} pipeline. "
            "Choose another collection name for this dataset."
        )
    collection = await find_by_name(kap, tenant_id, "collections", want["collection"])
    if collection is None:
        if records:
            etype = await ensure_entity_type(kap, tenant_id)
            body = records_collection_body(want["collection"], want["pipeline"], want["connector"], etype, settings.kap_chat_model_ref)
        else:
            body = collection_body(want["collection"], want["pipeline"], want["connector"])
        collection = await kap.request(tenant_id, "POST", "/api/collections", json=body)
    pipeline_id = pipeline.get("id")
    # an existing collection might reference another pipeline: follow what KAP says
    ref = collection.get("pipeline_ref")
    if ref and ref != want["pipeline"]:
        other = await find_by_name(kap, tenant_id, "pipelines", ref)
        pipeline_id = (other or {}).get("id", pipeline_id)
    return {
        **want,
        "kind": kind,
        "pipeline": ref or want["pipeline"],
        "connector_id": connector.get("id"),
        "collection_id": collection.get("id"),
        "pipeline_id": pipeline_id,
        "provisioned_at": datetime.now(timezone.utc).isoformat(),
    }
