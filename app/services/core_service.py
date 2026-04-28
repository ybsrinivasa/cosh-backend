from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from sqlalchemy.orm import selectinload
from fastapi import HTTPException
from app.models.models import (
    Folder, Core, CoreDataItem, CoreDataTranslation,
    ConnectDataItem, ConnectDataPosition, StatusEnum
)
from app.neo4j_db import driver
from app.dependencies import is_stocker_only


async def name_is_unique_for_core(db: AsyncSession, name: str, exclude_core_id: str = None) -> bool:
    """Core and Folder names share one global namespace."""
    folder_exists = (await db.execute(select(Folder).where(Folder.name == name))).scalar_one_or_none()
    if folder_exists:
        return False

    core_q = select(Core).where(Core.name == name)
    if exclude_core_id:
        core_q = core_q.where(Core.id != exclude_core_id)
    core_exists = (await db.execute(core_q)).scalar_one_or_none()
    return core_exists is None


async def get_core(db: AsyncSession, core_id: str, current_user=None) -> Core:
    result = await db.execute(select(Core).where(Core.id == core_id))
    core = result.scalar_one_or_none()
    if not core:
        raise HTTPException(status_code=404, detail="Core not found")
    if current_user and is_stocker_only(current_user):
        if core.assigned_stocker_id != current_user.id:
            raise HTTPException(status_code=403, detail="You are not assigned to this Core")
    return core


async def get_item(db: AsyncSession, item_id: str) -> CoreDataItem:
    result = await db.execute(
        select(CoreDataItem)
        .options(selectinload(CoreDataItem.translations))
        .where(CoreDataItem.id == item_id)
    )
    item = result.scalar_one_or_none()
    if not item:
        raise HTTPException(status_code=404, detail="Core data item not found")
    return item


async def dual_write_create(db: AsyncSession, item: CoreDataItem):
    """BL-C-01: Write CoreDataItem to Neo4J after PostgreSQL insert."""
    with driver.session() as neo_session:
        neo_session.run(
            """
            CREATE (:CoreDataItem {
                id: $id,
                core_id: $core_id,
                english_value: $english_value,
                status: 'ACTIVE'
            })
            """,
            id=item.id,
            core_id=item.core_id,
            english_value=item.english_value,
        )


async def dual_write_update_english(item_id: str, english_value: str):
    """Update english_value on Neo4J node when English text changes."""
    with driver.session() as neo_session:
        neo_session.run(
            "MATCH (n:CoreDataItem {id: $id}) SET n.english_value = $val",
            id=item_id, val=english_value
        )


async def inactivity_cascade(db: AsyncSession, item_id: str):
    """BL-C-02: Cascade inactivity to all Connect Data Items referencing this item."""
    positions = await db.execute(
        select(ConnectDataPosition.connect_data_item_id)
        .where(ConnectDataPosition.core_data_item_id == item_id)
        .distinct()
    )
    connect_data_item_ids = [row[0] for row in positions.fetchall()]

    if connect_data_item_ids:
        await db.execute(
            update(ConnectDataItem)
            .where(ConnectDataItem.id.in_(connect_data_item_ids))
            .values(status=StatusEnum.INACTIVE)
        )
        with driver.session() as neo_session:
            for cdi_id in connect_data_item_ids:
                neo_session.run(
                    "MATCH ()-[r {connect_data_item_id: $id}]-() SET r.status = 'INACTIVE'",
                    id=cdi_id
                )

    with driver.session() as neo_session:
        neo_session.run(
            "MATCH (n:CoreDataItem {id: $id}) SET n.status = 'INACTIVE'",
            id=item_id
        )

    return len(connect_data_item_ids)
