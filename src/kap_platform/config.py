"""Settings (environment, prefix PLATFORM_) and the tenants file.

The tenants file lists the KAP tenants this layer manages and how it signs in
to KAP for each. It holds no secrets: credentials are named environment
variables (later: Vault references).
"""
from __future__ import annotations

import os
import socket
from functools import lru_cache
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class TokenAuth(BaseModel):
    """Static bearer token: a legacy KAP user API token."""

    type: Literal["token"]
    token_env: str

    def token(self) -> str:
        value = os.environ.get(self.token_env, "")
        if not value:
            raise RuntimeError(f"environment variable {self.token_env} is not set")
        return value


class M2MAuth(BaseModel):
    """OAuth 2.0 client credentials of a KAP service account (POST /oauth/token)."""

    type: Literal["m2m"]
    client_id: str
    client_secret_env: str

    def client_secret(self) -> str:
        value = os.environ.get(self.client_secret_env, "")
        if not value:
            raise RuntimeError(f"environment variable {self.client_secret_env} is not set")
        return value


class Tenant(BaseModel):
    id: str
    name: str
    auth: TokenAuth | M2MAuth = Field(discriminator="type")
    sync_jobs: bool = True  # mirror this tenant's agent jobs and link traces


class TenantsFile(BaseModel):
    tenants: list[Tenant] = []


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PLATFORM_", env_file=".env", extra="ignore")

    database_url: str = "postgresql://platform:platform@localhost:5432/platform"
    kap_base_url: str = "http://127.0.0.1:8013"
    kap_timeout_s: float = 30.0
    tenants_file: str = "tenants.yaml"

    # Gateway: where model calls are forwarded. Route: /t/{tenant_id}/p/{provider}/v1/...
    gateway_default_upstream: str = "http://sceai-stand-vllm:8000"
    gateway_upstreams: dict[str, str] = {}  # provider name → upstream origin, overrides the default
    gateway_timeout_s: float = 600.0

    # API
    api_keys: list[str] = []  # bearer keys accepted from the console (slice 1; replaced by our IAM)
    hook_token: str = ""  # shared secret KAP sends back in callback_headers
    public_base_url: str = "http://platform-api:8080"  # how KAP reaches our callback endpoint

    # Worker
    worker_id: str = Field(default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}")
    sync_interval_s: int = 15
    lease_s: int = 60
    correlation_slack_s: float = 2.0

    def tenants(self) -> list[Tenant]:
        return load_tenants(self.tenants_file)

    def upstream_for(self, provider: str) -> str:
        return self.gateway_upstreams.get(provider, self.gateway_default_upstream).rstrip("/")


def load_tenants(path: str) -> list[Tenant]:
    p = Path(path)
    if not p.exists():
        return []
    data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    return TenantsFile.model_validate(data).tenants


@lru_cache
def get_settings() -> Settings:
    return Settings()
