from __future__ import annotations

from hashlib import sha256


def migration_content_hash(sql: str) -> str:
    return sha256(sql.encode("utf-8")).hexdigest()
