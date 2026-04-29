"""
BL-C-06: Similarity Review Actions.
Async service used by the FastAPI router.
"""
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update
from fastapi import HTTPException
from app.models.models import (
    SimilarityPair, SimilarityStatus, CoreDataItem, Core,
    AuditLog, CoreLanguageConfig, StatusEnum, ConnectDataPosition,
)
from app.services.core_service import inactivity_cascade
from app.neo4j_db import driver


async def get_pending_queue(db: AsyncSession, limit: int = 100):
    """
    Return (total_pending, pairs, items_map, cores_map) for the review queue.
    Pairs ordered by similarity_score DESC, capped at `limit`.
    Uses batch queries — no N+1.
    """
    pairs = (await db.execute(
        select(SimilarityPair)
        .where(SimilarityPair.status == SimilarityStatus.PENDING)
        .order_by(SimilarityPair.similarity_score.desc())
        .limit(limit)
    )).scalars().all()

    total = (await db.execute(
        select(func.count()).select_from(SimilarityPair)
        .where(SimilarityPair.status == SimilarityStatus.PENDING)
    )).scalar_one()

    items_map: dict = {}
    cores_map: dict = {}

    if pairs:
        all_item_ids = list({p.item_id_a for p in pairs} | {p.item_id_b for p in pairs})
        items = (await db.execute(
            select(CoreDataItem).where(CoreDataItem.id.in_(all_item_ids))
        )).scalars().all()
        items_map = {item.id: item for item in items}

        core_ids = list({item.core_id for item in items_map.values()})
        cores = (await db.execute(
            select(Core).where(Core.id.in_(core_ids))
        )).scalars().all()
        cores_map = {core.id: core for core in cores}

    return total, pairs, items_map, cores_map


async def get_pair_for_review(db: AsyncSession, pair_id: str) -> SimilarityPair:
    pair = (await db.execute(
        select(SimilarityPair).where(SimilarityPair.id == pair_id)
    )).scalar_one_or_none()
    if not pair:
        raise HTTPException(status_code=404, detail="Similarity pair not found")
    if pair.status != SimilarityStatus.PENDING:
        raise HTTPException(status_code=409, detail=f"Pair already actioned: {pair.status.value}")
    return pair


async def action_keep_both(db: AsyncSession, pair: SimilarityPair, user_id: str):
    """BL-C-06 action 1: keep both items, mark pair as reviewed."""
    _stamp_review(pair, SimilarityStatus.KEEP_BOTH, user_id)
    db.add(AuditLog(
        user_id=user_id, action="SIMILARITY_KEEP_BOTH",
        entity_type="similarity_pairs", entity_id=pair.id,
    ))
    await db.commit()


async def action_remove_one(db: AsyncSession, pair: SimilarityPair, remove_item_id: str, user_id: str):
    """BL-C-06 action 2: inactivate one item and cascade via BL-C-02."""
    if remove_item_id not in (pair.item_id_a, pair.item_id_b):
        raise HTTPException(status_code=422, detail="remove_item_id must be one of the two pair items")

    item = (await db.execute(
        select(CoreDataItem).where(CoreDataItem.id == remove_item_id)
    )).scalar_one_or_none()
    if not item:
        raise HTTPException(status_code=404, detail="Item to remove not found")

    item.status = StatusEnum.INACTIVE
    await inactivity_cascade(db, remove_item_id)

    _stamp_review(pair, SimilarityStatus.REMOVE_ONE, user_id)
    db.add(AuditLog(
        user_id=user_id, action="SIMILARITY_REMOVE_ONE",
        entity_type="similarity_pairs", entity_id=pair.id,
        details={"removed_item_id": remove_item_id},
    ))
    await db.commit()


async def action_merge(db: AsyncSession, pair: SimilarityPair, canonical_value: str, user_id: str):
    """
    BL-C-06 action 3: merge two items into one canonical value.
    - Item A survives with the canonical value.
    - Item B is inactivated (BL-C-02 cascade).
    - Neo4J relationships from B are transferred to A via APOC.
    - Re-translation triggered for item A.
    """
    canonical = canonical_value.strip()
    if not canonical:
        raise HTTPException(status_code=422, detail="canonical_value must not be empty")

    item_a = (await db.execute(
        select(CoreDataItem).where(CoreDataItem.id == pair.item_id_a)
    )).scalar_one_or_none()
    item_b = (await db.execute(
        select(CoreDataItem).where(CoreDataItem.id == pair.item_id_b)
    )).scalar_one_or_none()
    if not item_a or not item_b:
        raise HTTPException(status_code=404, detail="One or both items not found")

    # (a) Update item A's english value and sync Neo4J node
    item_a.english_value = canonical
    with driver.session() as neo_session:
        neo_session.run(
            "MATCH (n:CoreDataItem {id: $id}) SET n.english_value = $val",
            id=item_a.id, val=canonical,
        )

    # (b) Re-point all ConnectDataPosition rows from item B → item A BEFORE cascade.
    #     This preserves every Connect Data row that referenced B — they now reference A.
    #     inactivity_cascade(B) then finds nothing to inactivate (correct per doc §10.3).
    await db.execute(
        update(ConnectDataPosition)
        .where(ConnectDataPosition.core_data_item_id == pair.item_id_b)
        .values(core_data_item_id=pair.item_id_a)
    )
    await db.flush()

    # (c) Transfer Neo4J relationships from B → A before item B is inactivated
    _transfer_neo4j_relationships(pair.item_id_b, pair.item_id_a)

    # (d) Inactivate item B — cascade finds no positions referencing B, so no Connect Data rows are lost
    item_b.status = StatusEnum.INACTIVE
    await inactivity_cascade(db, pair.item_id_b)

    # (d) Re-translate item A with the new canonical value
    lang_configs = (await db.execute(
        select(CoreLanguageConfig).where(CoreLanguageConfig.core_id == item_a.core_id)
    )).scalars().all()
    if lang_configs:
        from app.tasks.translation import translate_item
        translate_item.delay(item_a.id, canonical, [c.language_code for c in lang_configs])

    pair.merged_canonical_value = canonical
    _stamp_review(pair, SimilarityStatus.MERGED, user_id)
    db.add(AuditLog(
        user_id=user_id, action="SIMILARITY_MERGE",
        entity_type="similarity_pairs", entity_id=pair.id,
        details={"canonical_value": canonical, "removed_item_id": pair.item_id_b},
    ))
    await db.commit()


async def action_ignore(db: AsyncSession, pair: SimilarityPair, user_id: str):
    """BL-C-06 action 4: mark pair as ignored — will never appear in queue again."""
    _stamp_review(pair, SimilarityStatus.IGNORED, user_id)
    db.add(AuditLog(
        user_id=user_id, action="SIMILARITY_IGNORE",
        entity_type="similarity_pairs", entity_id=pair.id,
    ))
    await db.commit()


def _stamp_review(pair: SimilarityPair, status: SimilarityStatus, user_id: str):
    pair.status = status
    pair.reviewed_by = user_id
    pair.reviewed_at = datetime.now(timezone.utc)


def _transfer_neo4j_relationships(item_id_b: str, item_id_a: str):
    """
    Transfer all Neo4J relationships from item B to item A using APOC.
    Outgoing (b→target) and incoming (source→b) are handled separately.
    Called before BL-C-02 cascade so relationships still exist on B.
    """
    with driver.session() as neo_session:
        # Outgoing: (b)-[r]->(target) → create (a)-[r]->(target)
        neo_session.run(
            """
            MATCH (a:CoreDataItem {id: $id_a}), (b:CoreDataItem {id: $id_b})
            MATCH (b)-[r]->(target)
            WHERE NOT target.id = $id_b
            WITH a, target, type(r) AS rel_type,
                 coalesce(r.connect_data_item_id, '') AS cdi_id,
                 coalesce(r.connect_id, '') AS conn_id,
                 coalesce(r.schema_position_from, 0) AS pos_from,
                 coalesce(r.schema_position_to, 0) AS pos_to
            CALL apoc.create.relationship(a, rel_type, {
                connect_data_item_id: cdi_id,
                connect_id: conn_id,
                status: 'ACTIVE',
                schema_position_from: pos_from,
                schema_position_to: pos_to
            }, target) YIELD rel
            RETURN count(rel) AS transferred
            """,
            id_a=item_id_a, id_b=item_id_b,
        )
        # Incoming: (source)-[r]->(b) → create (source)-[r]->(a)
        neo_session.run(
            """
            MATCH (a:CoreDataItem {id: $id_a}), (b:CoreDataItem {id: $id_b})
            MATCH (source)-[r]->(b)
            WHERE NOT source.id = $id_a
            WITH source, a, type(r) AS rel_type,
                 coalesce(r.connect_data_item_id, '') AS cdi_id,
                 coalesce(r.connect_id, '') AS conn_id,
                 coalesce(r.schema_position_from, 0) AS pos_from,
                 coalesce(r.schema_position_to, 0) AS pos_to
            CALL apoc.create.relationship(source, rel_type, {
                connect_data_item_id: cdi_id,
                connect_id: conn_id,
                status: 'ACTIVE',
                schema_position_from: pos_from,
                schema_position_to: pos_to
            }, a) YIELD rel
            RETURN count(rel) AS transferred
            """,
            id_a=item_id_a, id_b=item_id_b,
        )
