import hashlib
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from app.config import settings

engine = create_async_engine(settings.database_url, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session


# Maximum time to wait for an entity lock before failing fast with a clear
# message. Long enough to absorb the tail of a normal upload that's about to
# commit; short enough that a stuck worker doesn't cascade into a queue of
# blocked retries that all 504 silently (the Pest Diagnosis upload pile-up
# on 2026-05-13).
_ENTITY_LOCK_TIMEOUT_MS = 30_000


async def acquire_entity_lock(db: AsyncSession, namespace: str, entity_id: str) -> None:
    """Serialise long-running mutations on one entity. Two concurrent uploads
    to the same Core or Connect can otherwise both load `existing_fingerprints`
    before either commits and end up double-inserting (the exact pattern that
    produced the Trade Names doubling on 2026-05-12 and the earlier Location
    Connect doubling on 2026-05-09).

    Held for the rest of the transaction; releases automatically on commit
    or rollback. If another worker is already holding the lock we wait up to
    `_ENTITY_LOCK_TIMEOUT_MS`, then raise 409 with a clear message — better
    than letting a queue of blocked workers pile up and 504 silently.
    """
    digest = hashlib.blake2b(f"{namespace}:{entity_id}".encode(), digest_size=8).digest()
    key = int.from_bytes(digest, "big", signed=True)

    # `SET LOCAL` only applies inside the current transaction, so the
    # following advisory-lock acquire will obey the bound. asyncpg won't
    # accept lock_timeout as a bind parameter, so the value is interpolated
    # from a constant we control — not user input.
    await db.execute(text(f"SET LOCAL lock_timeout = '{_ENTITY_LOCK_TIMEOUT_MS}ms'"))
    try:
        await db.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})
    except DBAPIError as e:
        if "lock_timeout" in str(e).lower() or "55P03" in str(e):
            await db.rollback()
            raise HTTPException(
                status_code=409,
                detail=(
                    "Another upload to this entity is already in progress. "
                    "Wait for it to finish (or ask an admin to restart the api "
                    "container if it's been stuck for more than a few minutes), "
                    "then retry."
                ),
            )
        raise
    # Reset the timeout so the rest of the upload's queries aren't artificially
    # capped — only the lock acquire itself needs the bound.
    await db.execute(text("SET LOCAL lock_timeout = '0'"))
