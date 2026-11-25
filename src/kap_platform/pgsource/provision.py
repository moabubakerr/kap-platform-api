"""KAP objects for a dataset: S3 connector, indicators pipeline, collection.

Created once per dataset and reused by name (idempotent): if an object with
the expected name already exists in the tenant it is used as is, never
patched (patching a pipeline makes KAP reprocess every collection using it).

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


def names(collection_name: str) -> dict[str, str]:
    return {"connector": f"{collection_name}_pg", "pipeline": f"{collection_name}_pipeline", "collection": collection_name}


def connector_body(name: str, tenant_id: str, dataset_name: str, settings: Settings) -> dict[str, Any]:
    endpoint = (settings.s3_kap_url or settings.s3_url).rstrip("/")
    tls = endpoint.lower().startswith("https://")
    return {
        "name": name,
        "type": "s3",
        "settings": {
            "object_selector": {"prefix": object_prefix(tenant_id, dataset_name), "include_globs": [f"{n}.csv" for n in FILES], "exclude_globs": []},
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


def collection_body(name: str, pipeline: str, connector: str) -> dict[str, Any]:
    return {"name": name, "pipeline_ref": pipeline, "connectors": [connector], "sync_strategy": "overwrite", "entity": {"enabled": False}}


async def find_by_name(kap: KapClient, tenant_id: str, resource: str, name: str) -> dict[str, Any] | None:
    async for item in kap.paginate(tenant_id, f"/api/{resource}"):
        if item.get("name") == name:
            return item
    return None


async def ensure(kap: KapClient, tenant_id: str, dataset: Mapping[str, Any], settings: Settings) -> dict[str, Any]:
    """Make sure the dataset's connector, pipeline and collection exist. Returns the dataset's `kap` record."""
    want = names(dataset["collection_name"])
    current = dataset.get("kap") or {}
    if current.get("collection_id") and current.get("pipeline_id") and all(current.get(k) == v for k, v in want.items()):
        return dict(current)

    connector = await find_by_name(kap, tenant_id, "connectors", want["connector"])
    if connector is None:
        connector = await kap.request(tenant_id, "POST", "/api/connectors", json=connector_body(want["connector"], tenant_id, dataset["name"], settings))
    pipeline = await find_by_name(kap, tenant_id, "pipelines", want["pipeline"])
    if pipeline is None:
        pipeline = await kap.request(tenant_id, "POST", "/api/pipelines", json=pipeline_body(want["pipeline"], settings.kap_embedding_model_ref))
    collection = await find_by_name(kap, tenant_id, "collections", want["collection"])
    if collection is None:
        collection = await kap.request(tenant_id, "POST", "/api/collections", json=collection_body(want["collection"], want["pipeline"], want["connector"]))
    pipeline_id = pipeline.get("id")
    # an existing collection might reference another pipeline: follow what KAP says
    ref = collection.get("pipeline_ref")
    if ref and ref != want["pipeline"]:
        other = await find_by_name(kap, tenant_id, "pipelines", ref)
        pipeline_id = (other or {}).get("id", pipeline_id)
    return {
        **want,
        "pipeline": ref or want["pipeline"],
        "connector_id": connector.get("id"),
        "collection_id": collection.get("id"),
        "pipeline_id": pipeline_id,
        "provisioned_at": datetime.now(timezone.utc).isoformat(),
    }
