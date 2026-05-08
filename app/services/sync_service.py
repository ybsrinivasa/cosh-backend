"""
BL-C-07: Sync Change Tracking and Dispatch.
"""
import uuid
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update
from fastapi import HTTPException
from app.models.models import (
    SyncChangeLog, SyncHistory, ProductSyncState, ProductRegistry,
    CoreProductTag, ConnectProductTag, Core, Connect, CoreType,
    CoreDataItem, CoreDataTranslation, ConnectDataItem, ConnectDataPosition,
    ConnectSchemaPosition, RelationshipTypeRegistry, MediaItem,
    EntityType, ChangeType, SyncMode, SyncStatus, StatusEnum,
)

# Dependency ordering from RootsTalk Sync API Contract §5.1
_ENTITY_ORDER = [
    "state", "crop_group", "organisation_type", "start_date_label",
    "expertise_domain", "cultivation_type", "application_method",
    "dosage_unit", "brand_unit", "volume_unit", "distance_unit",
    "time_unit", "temperature_unit", "number_unit", "irrigation_unit",
    "district", "crop", "manufacturer", "common_name_pesticide",
    "common_name_fertiliser", "formulation", "ai_concentration",
    "plant_part", "symptom", "problem_group", "pop_parameter",
    "sub_district", "brand", "sub_part", "sub_symptom",
    "specific_problem", "pop_variable", "maturity_index",
    "itk_name", "planting_material", "dus_character",
    "problem_stage", "crop_stage",
    "brand_to_common_name", "brand_to_manufacturer",
    "brand_to_formulation", "brand_to_concentration",
    "problem_to_crop", "problem_to_stage", "problem_to_symptom",
    "crop_to_stage", "symptom_image", "crop_to_parameter",
    "parameter_to_variable", "crop_to_dus", "maturity_to_crop",
]


async def write_sync_changes(
    db: AsyncSession,
    entity_type: EntityType,
    entity_id: str,
    change_type: ChangeType,
    core_id: str = None,
    connect_id: str = None,
):
    """
    P6-01: Insert one sync_change_log row per product that has this Core/Connect tagged.
    Called after every write operation. Does not commit — caller commits.
    """
    if core_id:
        tags = (await db.execute(
            select(CoreProductTag.product_id).where(CoreProductTag.core_id == core_id)
        )).scalars().all()
    elif connect_id:
        tags = (await db.execute(
            select(ConnectProductTag.product_id).where(ConnectProductTag.connect_id == connect_id)
        )).scalars().all()
    else:
        return

    for pid in tags:
        db.add(SyncChangeLog(
            product_id=pid,
            entity_type=entity_type,
            entity_id=entity_id,
            change_type=change_type,
        ))


async def get_product(db: AsyncSession, product_id: str) -> ProductRegistry:
    product = (await db.execute(
        select(ProductRegistry).where(ProductRegistry.id == product_id)
    )).scalar_one_or_none()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    return product


async def get_change_table(db: AsyncSession, product_id: str) -> dict:
    """
    P6-02 step 2: Return the change table — one summary row per affected Core or Connect.
    Groups sync_change_log entries by the parent Core or Connect.
    """
    product = await get_product(db, product_id)

    pending = (await db.execute(
        select(SyncChangeLog).where(
            SyncChangeLog.product_id == product_id,
            SyncChangeLog.included_in_sync_id.is_(None),
        )
    )).scalars().all()

    # Map entity_id → (entity_name, entity_category, set of change_types)
    grouped: dict[str, dict] = {}

    # Collect all item IDs by type for batch lookup
    core_item_ids = [r.entity_id for r in pending if r.entity_type in (EntityType.CORE_DATA_ITEM, EntityType.TRANSLATION)]
    connect_item_ids = [r.entity_id for r in pending if r.entity_type == EntityType.CONNECT_DATA_ITEM]

    # Batch lookup: core_data_item → core
    core_item_map: dict[str, str] = {}  # item_id → core_id
    if core_item_ids:
        rows = (await db.execute(
            select(CoreDataItem.id, CoreDataItem.core_id).where(CoreDataItem.id.in_(core_item_ids))
        )).all()
        core_item_map = {r[0]: r[1] for r in rows}

    # Batch lookup: cores
    all_core_ids = list(set(core_item_map.values()))
    core_name_map: dict[str, str] = {}
    if all_core_ids:
        rows = (await db.execute(
            select(Core.id, Core.name).where(Core.id.in_(all_core_ids))
        )).all()
        core_name_map = {r[0]: r[1] for r in rows}

    # Batch lookup: connect_data_item → connect
    connect_item_map: dict[str, str] = {}  # cdi_id → connect_id
    if connect_item_ids:
        rows = (await db.execute(
            select(ConnectDataItem.id, ConnectDataItem.connect_id).where(ConnectDataItem.id.in_(connect_item_ids))
        )).all()
        connect_item_map = {r[0]: r[1] for r in rows}

    all_connect_ids = list(set(connect_item_map.values()))
    connect_name_map: dict[str, str] = {}
    if all_connect_ids:
        rows = (await db.execute(
            select(Connect.id, Connect.name).where(Connect.id.in_(all_connect_ids))
        )).all()
        connect_name_map = {r[0]: r[1] for r in rows}

    # Build grouped summary
    for row in pending:
        if row.entity_type in (EntityType.CORE_DATA_ITEM, EntityType.TRANSLATION):
            core_id = core_item_map.get(row.entity_id)
            if not core_id:
                continue
            key = f"core:{core_id}"
            if key not in grouped:
                grouped[key] = {
                    "entity_id": core_id,
                    "entity_name": core_name_map.get(core_id, core_id),
                    "entity_category": "Core",
                    "change_types": set(),
                    "item_count": 0,
                }
            grouped[key]["change_types"].add(row.change_type.value)
            grouped[key]["item_count"] += 1

        elif row.entity_type == EntityType.CONNECT_DATA_ITEM:
            connect_id = connect_item_map.get(row.entity_id)
            if not connect_id:
                continue
            key = f"connect:{connect_id}"
            if key not in grouped:
                grouped[key] = {
                    "entity_id": connect_id,
                    "entity_name": connect_name_map.get(connect_id, connect_id),
                    "entity_category": "Connect",
                    "change_types": set(),
                    "item_count": 0,
                }
            grouped[key]["change_types"].add(row.change_type.value)
            grouped[key]["item_count"] += 1

    entries = [
        {
            "entity_id": v["entity_id"],
            "entity_name": v["entity_name"],
            "entity_category": v["entity_category"],
            "change_types": sorted(v["change_types"]),
            "item_count": v["item_count"],
        }
        for v in grouped.values()
    ]

    return {
        "product_id": product_id,
        "product_name": product.display_name,
        "total_changed_entities": len(entries),
        "entities": entries,
    }


async def resolve_dispatch_entities(
    db: AsyncSession,
    product_id: str,
    entity_ids: list[str],
    send_all: bool,
) -> tuple[list[str], list[str], list[str]]:
    """
    Returns (core_ids, connect_ids, auto_added_dependency_names).
    Auto-adds Core dependencies for any selected Connect.
    """
    if send_all:
        core_tags = (await db.execute(
            select(CoreProductTag.core_id).where(CoreProductTag.product_id == product_id)
        )).scalars().all()
        connect_tags = (await db.execute(
            select(ConnectProductTag.connect_id).where(ConnectProductTag.product_id == product_id)
        )).scalars().all()
        core_ids = list(core_tags)
        connect_ids = list(connect_tags)
    else:
        # Determine which entity_ids are Cores vs Connects
        tagged_cores = (await db.execute(
            select(CoreProductTag.core_id).where(
                CoreProductTag.product_id == product_id,
                CoreProductTag.core_id.in_(entity_ids),
            )
        )).scalars().all()
        tagged_connects = (await db.execute(
            select(ConnectProductTag.connect_id).where(
                ConnectProductTag.product_id == product_id,
                ConnectProductTag.connect_id.in_(entity_ids),
            )
        )).scalars().all()
        core_ids = list(tagged_cores)
        connect_ids = list(tagged_connects)

    # Auto-dependency: for each Connect, ensure its schema Cores are included
    auto_added_names = []
    for connect_id in connect_ids:
        schema_positions = (await db.execute(
            select(ConnectSchemaPosition.core_id).where(ConnectSchemaPosition.connect_id == connect_id)
        )).scalars().all()

        for dep_core_id in schema_positions:
            if dep_core_id not in core_ids:
                # Check if it's tagged to the product
                is_tagged = (await db.execute(
                    select(CoreProductTag).where(
                        CoreProductTag.core_id == dep_core_id,
                        CoreProductTag.product_id == product_id,
                    )
                )).scalar_one_or_none()
                if is_tagged:
                    core = (await db.execute(select(Core).where(Core.id == dep_core_id))).scalar_one_or_none()
                    core_ids.append(dep_core_id)
                    if core:
                        auto_added_names.append(core.name)

    return core_ids, connect_ids, auto_added_names


async def build_payload(
    db: AsyncSession,
    product_id: str,
    core_ids: list[str],
    connect_ids: list[str],
    sync_mode: SyncMode,
    sync_id: str,
    initiated_by: str,
) -> dict:
    """
    Build the enriched JSON payload for RootsTalk.
    Payload version 2.0 — relationship-type-aware, self-describing.

    Core batches: entity items with all language translations.
    Connect batches: schema definition (with relationship types) +
                     structured positions on every data row.
    RootsTalk can derive its entire schema from this payload.
    """
    product = await get_product(db, product_id)

    # ── Entity type labels ─────────────────────────────────────────────────────
    core_tags = (await db.execute(
        select(CoreProductTag).where(
            CoreProductTag.product_id == product_id,
            CoreProductTag.core_id.in_(core_ids),
        )
    )).scalars().all()
    core_label_map = {t.core_id: t.entity_type_label for t in core_tags}

    connect_tags = (await db.execute(
        select(ConnectProductTag).where(
            ConnectProductTag.product_id == product_id,
            ConnectProductTag.connect_id.in_(connect_ids),
        )
    )).scalars().all()
    connect_label_map = {t.connect_id: t.entity_type_label for t in connect_tags}

    # ── Relationship type display names (batch load once) ─────────────────────
    rel_type_rows = (await db.execute(select(RelationshipTypeRegistry))).scalars().all()
    rel_display_map = {r.label: r.display_name for r in rel_type_rows}

    # ── Changed IDs for incremental mode ──────────────────────────────────────
    changed_core_item_ids: set = set()
    changed_connect_item_ids: set = set()
    if sync_mode == SyncMode.INCREMENTAL:
        changed = (await db.execute(
            select(SyncChangeLog.entity_type, SyncChangeLog.entity_id).where(
                SyncChangeLog.product_id == product_id,
                SyncChangeLog.included_in_sync_id.is_(None),
            )
        )).all()
        for entity_type, entity_id in changed:
            if entity_type in (EntityType.CORE_DATA_ITEM, EntityType.TRANSLATION):
                changed_core_item_ids.add(entity_id)
            elif entity_type == EntityType.CONNECT_DATA_ITEM:
                changed_connect_item_ids.add(entity_id)

    batches: dict[str, dict] = {}

    # ── Core entity batches ────────────────────────────────────────────────────
    for core_id in core_ids:
        entity_type_label = core_label_map.get(core_id) or f"core_{core_id[:8]}"

        items_q = select(CoreDataItem).where(CoreDataItem.core_id == core_id)
        if sync_mode == SyncMode.INCREMENTAL:
            relevant_ids = {eid for eid in changed_core_item_ids}
            if not relevant_ids:
                continue
            items_q = items_q.where(CoreDataItem.id.in_(relevant_ids))
        else:
            items_q = items_q.where(CoreDataItem.status == StatusEnum.ACTIVE)

        items = (await db.execute(items_q)).scalars().all()
        if not items:
            continue

        # MEDIA cores: batch-fetch MediaItem rows so we can emit s3_path/media_type
        # in the per-item metadata. One query per Core, not per item.
        core_obj = (await db.execute(select(Core).where(Core.id == core_id))).scalar_one_or_none()
        is_media_core = core_obj is not None and core_obj.core_type == CoreType.MEDIA
        media_by_item: dict[str, MediaItem] = {}
        if is_media_core:
            item_ids = [i.id for i in items]
            media_rows = (await db.execute(
                select(MediaItem).where(MediaItem.item_id.in_(item_ids))
            )).scalars().all()
            for m in media_rows:
                media_by_item[m.item_id] = m

        if entity_type_label not in batches:
            batches[entity_type_label] = {"entity_type": entity_type_label, "items": []}

        for item in items:
            translations = {"en": item.english_value}
            trans_rows = (await db.execute(
                select(CoreDataTranslation).where(CoreDataTranslation.item_id == item.id)
            )).scalars().all()
            for t in trans_rows:
                translations[t.language_code] = t.translated_value

            metadata: dict = {}
            m = media_by_item.get(item.id)
            if m:
                if m.s3_url:
                    metadata["s3_path"] = m.s3_url
                if m.content_type:
                    ct_val = m.content_type.value if hasattr(m.content_type, "value") else str(m.content_type)
                    metadata["media_type"] = ct_val.lower()

            batches[entity_type_label]["items"].append({
                "cosh_id": item.id,
                "entity_type": entity_type_label,
                "status": "active" if item.status == StatusEnum.ACTIVE else "inactive",
                "translations": translations,
                "parent_cosh_id": None,
                "metadata": metadata,
            })

    # ── Connect entity batches ─────────────────────────────────────────────────
    for connect_id in connect_ids:
        entity_type_label = connect_label_map.get(connect_id) or f"connect_{connect_id[:8]}"

        connect_obj = (await db.execute(
            select(Connect).where(Connect.id == connect_id)
        )).scalar_one_or_none()

        # Load schema positions
        schema_positions = (await db.execute(
            select(ConnectSchemaPosition)
            .where(ConnectSchemaPosition.connect_id == connect_id)
            .order_by(ConnectSchemaPosition.position_number)
        )).scalars().all()

        # Build schema definition — this is the key new addition
        schema = []
        pos_entity_type_map: dict[int, str] = {}
        for sp in schema_positions:
            node_type = sp.node_type.value if hasattr(sp.node_type, 'value') else str(sp.node_type)
            if node_type == 'CONNECT':
                pos_entity_type = connect_label_map.get(sp.connect_ref_id) or f"connect_{(sp.connect_ref_id or '')[:8]}"
            else:
                pos_entity_type = core_label_map.get(sp.core_id) or f"core_{(sp.core_id or '')[:8]}"

            pos_entity_type_map[sp.position_number] = pos_entity_type

            schema.append({
                "position_number": sp.position_number,
                "node_type": node_type,
                "entity_type": pos_entity_type,
                "relationship_to_next": sp.relationship_type_to_next,
                "relationship_display_name": rel_display_map.get(sp.relationship_type_to_next)
                    if sp.relationship_type_to_next else None,
            })

        items_q = select(ConnectDataItem).where(
            ConnectDataItem.connect_id == connect_id,
            ConnectDataItem.status == StatusEnum.ACTIVE,
        )
        if sync_mode == SyncMode.INCREMENTAL:
            if not changed_connect_item_ids:
                continue
            items_q = items_q.where(ConnectDataItem.id.in_(changed_connect_item_ids))

        cdis = (await db.execute(items_q)).scalars().all()
        if not cdis:
            continue

        if entity_type_label not in batches:
            batches[entity_type_label] = {
                "entity_type": entity_type_label,
                "connect_id": connect_id,
                "connect_name": connect_obj.name if connect_obj else entity_type_label,
                "schema": schema,
                "items": [],
            }

        for cdi in cdis:
            data_positions = (await db.execute(
                select(ConnectDataPosition)
                .where(ConnectDataPosition.connect_data_item_id == cdi.id)
                .order_by(ConnectDataPosition.position_number)
            )).scalars().all()

            # Structured positions: position_number → {cosh_id, entity_type}
            positions_out = {}
            for p in data_positions:
                value_id = p.connect_data_item_ref_id or p.core_data_item_id
                positions_out[str(p.position_number)] = {
                    "cosh_id": value_id,
                    "entity_type": pos_entity_type_map.get(p.position_number,
                                                            f"position_{p.position_number}"),
                }

            batches[entity_type_label]["items"].append({
                "cosh_id": cdi.id,
                "entity_type": entity_type_label,
                "status": "active" if cdi.status == StatusEnum.ACTIVE else "inactive",
                "positions": positions_out,
            })

    # ── Sort by dependency order and return ───────────────────────────────────
    def _sort_key(label: str) -> int:
        try:
            return _ENTITY_ORDER.index(label)
        except ValueError:
            return len(_ENTITY_ORDER)

    sorted_batches = sorted(batches.values(), key=lambda b: _sort_key(b["entity_type"]))

    return {
        "cosh_payload_version": "2.0",
        "sync_id": sync_id,
        "product": product.name,
        "sync_mode": sync_mode.value.lower(),
        "initiated_at": datetime.now(timezone.utc).isoformat(),
        "initiated_by": initiated_by,
        "entity_batches": sorted_batches,
    }


async def create_sync_history(
    db: AsyncSession,
    product_id: str,
    sync_mode: SyncMode,
    initiated_by: str,
    sync_id: str,
) -> SyncHistory:
    history = SyncHistory(
        id=sync_id,
        product_id=product_id,
        sync_mode=sync_mode,
        initiated_by=initiated_by,
        status=SyncStatus.DISPATCHED,
    )
    db.add(history)
    await db.commit()
    await db.refresh(history)
    return history


async def get_sync_history(db: AsyncSession, product_id: str, limit: int = 20) -> list:
    await get_product(db, product_id)
    return (await db.execute(
        select(SyncHistory)
        .where(SyncHistory.product_id == product_id)
        .order_by(SyncHistory.initiated_at.desc())
        .limit(limit)
    )).scalars().all()
