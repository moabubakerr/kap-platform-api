"""Uploads to the S3-compatible store KAP's connector reads (RustFS on the stand).

boto3 is synchronous; every call runs in a worker thread. Path addressing,
no default request checksums (some S3-compatible servers reject the CRC
headers newer botocore adds).
"""
from __future__ import annotations

import asyncio
from typing import IO, Any

from ..config import Settings


class S3NotConfigured(RuntimeError):
    pass


def object_key(tenant_id: str, dataset_name: str, filename: str) -> str:
    return f"{tenant_id}/{dataset_name}/{filename}"


def record_key(tenant_id: str, dataset_name: str, path: str) -> str:
    """records preset: `path` is `<record slug>/record.docx` (one folder per record)."""
    return f"{tenant_id}/{dataset_name}/{path}"


def object_prefix(tenant_id: str, dataset_name: str) -> str:
    return f"{tenant_id}/{dataset_name}/"


class S3Store:
    def __init__(self, url: str, access_key: str, secret_key: str, bucket: str, region: str = "us-east-1"):
        self.url, self.bucket, self.region = url, bucket, region
        self._access_key, self._secret_key = access_key, secret_key
        self._client: Any = None
        self._bucket_ready = False

    @classmethod
    def from_settings(cls, settings: Settings) -> "S3Store":
        if not (settings.s3_access_key and settings.s3_secret_key):
            raise S3NotConfigured("PLATFORM_S3_ACCESS_KEY / PLATFORM_S3_SECRET_KEY are not set.")
        return cls(settings.s3_url, settings.s3_access_key, settings.s3_secret_key, settings.s3_bucket, settings.s3_region)

    def _c(self) -> Any:
        if self._client is None:
            import boto3
            from botocore.config import Config

            self._client = boto3.client(
                "s3",
                endpoint_url=self.url,
                aws_access_key_id=self._access_key,
                aws_secret_access_key=self._secret_key,
                region_name=self.region,
                config=Config(
                    signature_version="s3v4",
                    s3={"addressing_style": "path"},
                    connect_timeout=10,
                    read_timeout=60,
                    retries={"max_attempts": 3, "mode": "standard"},
                    request_checksum_calculation="when_required",
                    response_checksum_validation="when_required",
                ),
            )
        return self._client

    def _ensure_bucket(self) -> None:
        from botocore.exceptions import ClientError

        c = self._c()
        try:
            c.head_bucket(Bucket=self.bucket)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code not in ("404", "NoSuchBucket", "NotFound"):
                raise
            try:
                c.create_bucket(Bucket=self.bucket)
            except ClientError as exc2:
                if exc2.response.get("Error", {}).get("Code") not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                    raise

    async def ensure_bucket(self) -> None:
        if not self._bucket_ready:
            await asyncio.to_thread(self._ensure_bucket)
            self._bucket_ready = True

    async def put(self, key: str, body: IO[bytes], content_type: str = "text/csv; charset=utf-8") -> None:
        await self.ensure_bucket()
        await asyncio.to_thread(self._c().upload_fileobj, body, self.bucket, key, ExtraArgs={"ContentType": content_type})

    def _list(self, prefix: str) -> dict[str, str]:
        out: dict[str, str] = {}
        for page in self._c().get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            for o in page.get("Contents", []) or []:
                out[o["Key"]] = str(o.get("ETag") or "").strip('"').lower()
        return out

    async def list_keys(self, prefix: str) -> dict[str, str]:
        """key → ETag (lower-case, unquoted; MD5 for single-part uploads) of every object under the prefix."""
        await self.ensure_bucket()
        return await asyncio.to_thread(self._list, prefix)

    async def delete(self, key: str) -> None:
        """One DeleteObject per key (no DeleteObjects: it needs Content-MD5, which some S3-compatible servers handle differently)."""
        await asyncio.to_thread(self._c().delete_object, Bucket=self.bucket, Key=key)
