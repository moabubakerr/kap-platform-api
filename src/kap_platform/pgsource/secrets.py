"""Source passwords at rest: Fernet (AES-128-CBC + HMAC-SHA256) with PLATFORM_SECRET_KEY.

Generate a key:  python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
Losing or changing the key makes stored passwords unreadable (re-enter them).
"""
from __future__ import annotations

from cryptography.fernet import Fernet, InvalidToken


class SecretKeyMissing(RuntimeError):
    pass


class SecretBox:
    def __init__(self, key: str):
        if not key:
            raise SecretKeyMissing("PLATFORM_SECRET_KEY is not set; source passwords cannot be stored or read.")
        try:
            self._f = Fernet(key.encode() if isinstance(key, str) else key)
        except (ValueError, TypeError) as exc:
            raise SecretKeyMissing("PLATFORM_SECRET_KEY is not a valid Fernet key (urlsafe base64 of 32 bytes).") from exc

    def encrypt(self, plaintext: str) -> str:
        return self._f.encrypt(plaintext.encode("utf-8")).decode("ascii")

    def decrypt(self, token: str) -> str:
        try:
            return self._f.decrypt(token.encode("ascii")).decode("utf-8")
        except InvalidToken as exc:
            raise SecretKeyMissing("The stored password cannot be decrypted with the current PLATFORM_SECRET_KEY; enter it again.") from exc
