import hashlib
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import text
from app.config import settings

engine = create_async_engine(settings.database_url, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session


async def acquire_entity_lock(db: AsyncSession, namespace: str, entity_id: str) -> None:
    """Serialise long-running mutations on one entity. Two concurrent uploads
    to the same Core or Connect can otherwise both load `existing_fingerprints`
    before either commits and end up double-inserting (the exact pattern that
    produced the Trade Names doubling on 2026-05-12 and the earlier Location
    Connect doubling on 2026-05-09).

    `pg_advisory_xact_lock` holds the lock for the rest of the transaction
    and releases automatically on commit or rollback — no cleanup needed.
    """
    digest = hashlib.blake2b(f"{namespace}:{entity_id}".encode(), digest_size=8).digest()
    key = int.from_bytes(digest, "big", signed=True)
    await db.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})
