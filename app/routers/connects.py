import io
import uuid
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, insert
from sqlalchemy.orm import selectinload
from app.database import get_db
from app.dependencies import require_role, is_stocker_only, check_stocker_exclusive_write
from app.models.models import (
    Connect, ConnectSchemaPosition, ConnectDataItem, ConnectDataPosition,
    ConnectProductTag, ProductRegistry, CoreDataItem, Core, User,
    UserRole, StatusEnum, NodeType, SyncChangeLog
)
from app.schemas.connects import (
    ConnectCreate, ConnectUpdate, ConnectOut, SchemaPositionIn, SchemaPositionOut,
    ConnectDataPositionIn, ConnectDataItemOut, ConnectProductTagOut,
    ConnectStatusUpdate, ConnectDataStatusUpdate, ExcelUploadReport,
    DuplicatesResponse, DuplicateGroup, DuplicateRow, DuplicatePositionValue,
    DuplicateCleanupRequest, DuplicateCleanupResponse,
)
from app.services.connect_service import (
    get_connect, check_schema_uniqueness_with_connect_refs, validate_relationship_type,
    create_neo4j_relationships, inactivate_neo4j_relationships
)
from app.services.sync_service import write_sync_changes
from app.models.models import EntityType, ChangeType
from app.neo4j_db import driver as _neo_driver

router = APIRouter(prefix="/connects", tags=["Connects"])

require_designer = require_role(UserRole.DESIGNER, UserRole.ADMIN)
require_designer_or_stocker = require_role(UserRole.DESIGNER, UserRole.STOCKER, UserRole.ADMIN)


# ── Connect CRUD ───────────────────────────────────────────────────────────────

@router.get("", response_model=list[ConnectOut])
async def list_connects(db: AsyncSession = Depends(get_db), current_user=Depends(require_designer_or_stocker)):
    q = select(Connect).order_by(Connect.name)
    if is_stocker_only(current_user):
        q = q.where(Connect.assigned_stocker_id == current_user.id)
    result = await db.execute(q)
    return result.scalars().all()


@router.post("", response_model=ConnectOut, status_code=status.HTTP_201_CREATED)
async def create_connect(
    request: ConnectCreate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(require_designer),
):
    existing = (await db.execute(select(Connect).where(Connect.name == request.name))).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail=f"A Connect named '{request.name}' already exists")

    connect = Connect(
        name=request.name,
        description=request.description,
        status=StatusEnum.ACTIVE,
        schema_finalised=False,
        created_by=current_user.id,
    )
    db.add(connect)
    await db.commit()
    await db.refresh(connect)
    return connect


@router.get("/{connect_id}", response_model=ConnectOut)
async def get_connect_detail(connect_id: str, db: AsyncSession = Depends(get_db), current_user=Depends(require_designer_or_stocker)):
    return await get_connect(db, connect_id, current_user)


@router.put("/{connect_id}", response_model=ConnectOut)
async def update_connect(
    connect_id: str,
    request: ConnectUpdate,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_designer),
):
    connect = await get_connect(db, connect_id)

    if request.name and request.name != connect.name:
        existing = (await db.execute(select(Connect).where(Connect.name == request.name))).scalar_one_or_none()
        if existing:
            raise HTTPException(status_code=409, detail=f"A Connect named '{request.name}' already exists")
        connect.name = request.name

    if request.description is not None:
        connect.description = request.description
    if 'assigned_stocker_id' in request.model_fields_set:
        connect.assigned_stocker_id = request.assigned_stocker_id

    await db.commit()
    await db.refresh(connect)
    return connect


@router.put("/{connect_id}/status", response_model=ConnectOut)
async def update_connect_status(
    connect_id: str,
    request: ConnectStatusUpdate,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_designer),
):
    """Inactivate or reactivate a Connect. Inactivating also inactivates all its data rows."""
    connect = await get_connect(db, connect_id)
    connect.status = request.status

    if request.status == StatusEnum.INACTIVE:
        active_items = (await db.execute(
            select(ConnectDataItem).where(
                ConnectDataItem.connect_id == connect_id,
                ConnectDataItem.status == StatusEnum.ACTIVE,
            )
        )).scalars().all()
        for item in active_items:
            item.status = StatusEnum.INACTIVE
            inactivate_neo4j_relationships(item.id)

    await db.commit()
    await db.refresh(connect)
    return connect


# ── Connect Schema ─────────────────────────────────────────────────────────────

async def _enrich_schema_positions(db: AsyncSession, positions) -> list[SchemaPositionOut]:
    """Resolve Core/Connect names for schema positions."""
    core_ids = list({p.core_id for p in positions if p.core_id})
    core_name_map = {}
    if core_ids:
        cores = (await db.execute(select(Core.id, Core.name).where(Core.id.in_(core_ids)))).all()
        core_name_map = {c.id: c.name for c in cores}

    connect_ref_ids = list({p.connect_ref_id for p in positions if p.connect_ref_id})
    connect_name_map = {}
    if connect_ref_ids:
        connects = (await db.execute(select(Connect.id, Connect.name).where(Connect.id.in_(connect_ref_ids)))).all()
        connect_name_map = {c.id: c.name for c in connects}

    return [
        SchemaPositionOut(
            id=p.id,
            connect_id=p.connect_id,
            position_number=p.position_number,
            node_type=p.node_type.value if hasattr(p.node_type, 'value') else str(p.node_type),
            core_id=p.core_id,
            core_name=core_name_map.get(p.core_id) if p.core_id else None,
            connect_ref_id=p.connect_ref_id,
            connect_ref_name=connect_name_map.get(p.connect_ref_id) if p.connect_ref_id else None,
            relationship_type_to_next=p.relationship_type_to_next,
            position_label=p.position_label,
        )
        for p in positions
    ]


@router.get("/{connect_id}/schema", response_model=list[SchemaPositionOut])
async def get_schema(connect_id: str, db: AsyncSession = Depends(get_db), current_user=Depends(require_designer_or_stocker)):
    await get_connect(db, connect_id, current_user)
    result = await db.execute(
        select(ConnectSchemaPosition)
        .where(ConnectSchemaPosition.connect_id == connect_id)
        .order_by(ConnectSchemaPosition.position_number)
    )
    positions = result.scalars().all()
    if not positions:
        return []
    return await _enrich_schema_positions(db, positions)


@router.post("/{connect_id}/schema", response_model=list[SchemaPositionOut], status_code=status.HTTP_201_CREATED)
async def define_schema(
    connect_id: str,
    positions: list[SchemaPositionIn],
    db: AsyncSession = Depends(get_db),
    _=Depends(require_designer),
):
    connect = await get_connect(db, connect_id)

    if connect.schema_finalised:
        raise HTTPException(status_code=409, detail="Schema is locked — Connect Data has already been added to this Connect")

    if len(positions) < 2:
        raise HTTPException(status_code=422, detail="A Connect schema must have at least 2 positions")

    sorted_positions = sorted(positions, key=lambda p: p.position_number)

    for i, pos in enumerate(sorted_positions):
        is_last = (i == len(sorted_positions) - 1)

        # Validate exactly one of core_id / connect_ref_id is set
        has_core = bool(pos.core_id)
        has_connect = bool(pos.connect_ref_id)
        if not has_core and not has_connect:
            raise HTTPException(status_code=422, detail=f"Position {pos.position_number}: must specify either core_id or connect_ref_id")
        if has_core and has_connect:
            raise HTTPException(status_code=422, detail=f"Position {pos.position_number}: specify either core_id or connect_ref_id, not both")

        if is_last:
            if pos.relationship_type_to_next:
                raise HTTPException(status_code=422, detail="The last position must not have a relationship_type_to_next")
        else:
            if not pos.relationship_type_to_next:
                raise HTTPException(status_code=422, detail=f"Position {pos.position_number} must have a relationship_type_to_next")
            await validate_relationship_type(db, pos.relationship_type_to_next)

        if has_core:
            core_exists = (await db.execute(select(Core).where(Core.id == pos.core_id))).scalar_one_or_none()
            if not core_exists:
                raise HTTPException(status_code=404, detail=f"Core '{pos.core_id}' not found for position {pos.position_number}")
        else:
            if pos.connect_ref_id == connect_id:
                raise HTTPException(status_code=422, detail=f"Position {pos.position_number}: a Connect cannot reference itself")
            ref_connect = (await db.execute(select(Connect).where(Connect.id == pos.connect_ref_id))).scalar_one_or_none()
            if not ref_connect:
                raise HTTPException(status_code=404, detail=f"Connect '{pos.connect_ref_id}' not found for position {pos.position_number}")

    position_dicts = [
        {
            "position_number": p.position_number,
            "core_id": p.core_id,
            "connect_ref_id": p.connect_ref_id,
            "relationship_type_to_next": p.relationship_type_to_next,
        }
        for p in sorted_positions
    ]
    await check_schema_uniqueness_with_connect_refs(db, position_dicts, exclude_connect_id=connect_id)

    existing = await db.execute(select(ConnectSchemaPosition).where(ConnectSchemaPosition.connect_id == connect_id))
    for row in existing.scalars().all():
        await db.delete(row)

    for pos in sorted_positions:
        label = (pos.position_label or "").strip() or None
        schema_pos = ConnectSchemaPosition(
            connect_id=connect_id,
            position_number=pos.position_number,
            node_type=NodeType.CONNECT if pos.connect_ref_id else NodeType.CORE,
            core_id=pos.core_id,
            connect_ref_id=pos.connect_ref_id,
            relationship_type_to_next=pos.relationship_type_to_next,
            position_label=label,
        )
        db.add(schema_pos)

    await db.commit()
    result = await db.execute(
        select(ConnectSchemaPosition)
        .where(ConnectSchemaPosition.connect_id == connect_id)
        .order_by(ConnectSchemaPosition.position_number)
    )
    return await _enrich_schema_positions(db, result.scalars().all())


class _PositionLabelUpdate(BaseModel):
    position_label: Optional[str] = None


@router.put("/{connect_id}/schema/{position_id}/label", response_model=SchemaPositionOut)
async def update_schema_position_label(
    connect_id: str,
    position_id: str,
    request: _PositionLabelUpdate,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_designer),
):
    """Edit only a schema position's display label.
    Allowed even when the schema is finalised — labels are presentational
    (used to disambiguate duplicate-Core columns in the row builder and
    CSV upload), they don't change the schema's semantic structure."""
    await get_connect(db, connect_id)
    pos = (await db.execute(
        select(ConnectSchemaPosition).where(
            ConnectSchemaPosition.id == position_id,
            ConnectSchemaPosition.connect_id == connect_id,
        )
    )).scalar_one_or_none()
    if not pos:
        raise HTTPException(status_code=404, detail="Schema position not found")

    pos.position_label = (request.position_label or "").strip() or None
    await db.commit()

    enriched = await _enrich_schema_positions(db, [pos])
    return enriched[0]


# ── Connect Product Tags ───────────────────────────────────────────────────────

@router.get("/{connect_id}/product-tags", response_model=list[ConnectProductTagOut])
async def list_connect_product_tags(connect_id: str, db: AsyncSession = Depends(get_db), current_user=Depends(require_designer_or_stocker)):
    await get_connect(db, connect_id, current_user)
    result = await db.execute(select(ConnectProductTag).where(ConnectProductTag.connect_id == connect_id))
    return result.scalars().all()


@router.post("/{connect_id}/product-tags", response_model=ConnectProductTagOut, status_code=status.HTTP_201_CREATED)
async def tag_connect_to_product(
    connect_id: str,
    product_id: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_designer),
):
    await get_connect(db, connect_id)
    product = (await db.execute(select(ProductRegistry).where(ProductRegistry.id == product_id))).scalar_one_or_none()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    existing = (await db.execute(
        select(ConnectProductTag).where(ConnectProductTag.connect_id == connect_id, ConnectProductTag.product_id == product_id)
    )).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail="Connect already tagged to this product")

    tag = ConnectProductTag(connect_id=connect_id, product_id=product_id)
    db.add(tag)
    await db.commit()
    await db.refresh(tag)
    return tag


@router.delete("/{connect_id}/product-tags/{product_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_connect_product_tag(
    connect_id: str,
    product_id: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_designer),
):
    tag = (await db.execute(
        select(ConnectProductTag).where(ConnectProductTag.connect_id == connect_id, ConnectProductTag.product_id == product_id)
    )).scalar_one_or_none()
    if not tag:
        raise HTTPException(status_code=404, detail="Tag not found")
    await db.delete(tag)
    await db.commit()


# ── Connect Data Rows for Combobox ────────────────────────────────────────────

@router.get("/{connect_id}/data-rows")
async def get_connect_data_rows(
    connect_id: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(require_designer_or_stocker),
):
    """Return active data rows as labelled options for use in another Connect's data entry form."""
    schema_positions = (await db.execute(
        select(ConnectSchemaPosition)
        .where(ConnectSchemaPosition.connect_id == connect_id)
        .order_by(ConnectSchemaPosition.position_number)
    )).scalars().all()

    items = (await db.execute(
        select(ConnectDataItem)
        .options(selectinload(ConnectDataItem.positions))
        .where(ConnectDataItem.connect_id == connect_id, ConnectDataItem.status == StatusEnum.ACTIVE)
        .order_by(ConnectDataItem.created_at)
    )).scalars().all()

    # Batch-resolve all core_data_item_ids to English values
    all_cdi_ids = {pos.core_data_item_id for item in items for pos in item.positions if pos.core_data_item_id}
    value_map = {}
    if all_cdi_ids:
        rows = (await db.execute(
            select(CoreDataItem.id, CoreDataItem.english_value).where(CoreDataItem.id.in_(all_cdi_ids))
        )).all()
        value_map = {r.id: r.english_value for r in rows}

    result = []
    for item in items:
        pos_map = {p.position_number: p for p in item.positions}
        parts = []
        for sp in schema_positions:
            p = pos_map.get(sp.position_number)
            if p:
                if p.core_data_item_id:
                    parts.append(value_map.get(p.core_data_item_id, '?'))
                elif p.connect_data_item_ref_id:
                    parts.append(f'[nested:{p.connect_data_item_ref_id[:8]}]')
            else:
                parts.append('?')
        result.append({"id": item.id, "label": " — ".join(parts)})

    return result


# ── Connect Data Items — Manual Entry ─────────────────────────────────────────

@router.get("/{connect_id}/items", response_model=list[ConnectDataItemOut])
async def list_connect_data_items(
    connect_id: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(require_designer_or_stocker),
):
    await get_connect(db, connect_id, current_user)
    items = (await db.execute(
        select(ConnectDataItem)
        .options(selectinload(ConnectDataItem.positions))
        .where(ConnectDataItem.connect_id == connect_id)
        .order_by(ConnectDataItem.created_at)
    )).scalars().all()

    user_ids = list({item.created_by for item in items if item.created_by})
    user_map: dict = {}
    if user_ids:
        users = (await db.execute(
            select(User.id, User.name, User.email).where(User.id.in_(user_ids))
        )).all()
        user_map = {u.id: u.name or u.email for u in users}

    # Resolve core_data_item_ids to display names — no status filter so inactive items still resolve
    all_cdi_ids = {
        pos.core_data_item_id
        for item in items
        for pos in item.positions
        if pos.core_data_item_id
    }
    cdi_name_map: dict = {}
    if all_cdi_ids:
        rows = (await db.execute(
            select(CoreDataItem.id, CoreDataItem.english_value, CoreDataItem.status)
            .where(CoreDataItem.id.in_(all_cdi_ids))
        )).all()
        cdi_name_map = {r.id: {"name": r.english_value, "status": r.status.value} for r in rows}

    def _enrich_position(pos):
        enriched = {
            "position_number": pos.position_number,
            "core_data_item_id": pos.core_data_item_id,
            "connect_data_item_ref_id": pos.connect_data_item_ref_id,
        }
        if pos.core_data_item_id:
            info = cdi_name_map.get(pos.core_data_item_id, {})
            enriched["display_value"] = info.get("name", pos.core_data_item_id)
            enriched["item_status"] = info.get("status", "UNKNOWN")
        return enriched

    return [
        {
            "id": item.id,
            "connect_id": item.connect_id,
            "status": item.status,
            "created_by_name": item.legacy_created_by_name or user_map.get(item.created_by),
            "created_at": item.created_at,
            "positions": [_enrich_position(p) for p in item.positions],
        }
        for item in items
    ]


def _position_value_id(pos) -> str:
    """Return the value identifier for a data position regardless of type."""
    return pos.connect_data_item_ref_id or pos.core_data_item_id or ""


def _input_value_id(pos_in) -> str:
    """Return the value identifier from a ConnectDataPositionIn."""
    return pos_in.connect_data_item_ref_id or pos_in.core_data_item_id or ""


def _make_fingerprint(positions) -> str:
    return "|".join(
        f"{p.position_number}:{_position_value_id(p)}"
        for p in sorted(positions, key=lambda x: x.position_number)
    )


def _make_input_fingerprint(positions_in) -> str:
    return "|".join(
        f"{p.position_number}:{_input_value_id(p)}"
        for p in sorted(positions_in, key=lambda x: x.position_number)
    )


async def _validate_positions(db, positions_in, schema_positions, connect_id):
    """Validate each submitted position against the schema. Returns list of (position_number, value_id)."""
    if len(positions_in) != len(schema_positions):
        raise HTTPException(status_code=422, detail=f"Expected {len(schema_positions)} positions, got {len(positions_in)}")

    schema_map = {p.position_number: p for p in schema_positions}
    resolved = []
    seen_ids = []

    for pos in positions_in:
        if pos.position_number not in schema_map:
            raise HTTPException(status_code=422, detail=f"Position {pos.position_number} not in schema")

        schema_pos = schema_map[pos.position_number]
        node_type = schema_pos.node_type.value if hasattr(schema_pos.node_type, 'value') else str(schema_pos.node_type)

        if node_type == 'CORE':
            if not pos.core_data_item_id:
                raise HTTPException(status_code=422, detail=f"Position {pos.position_number}: core_data_item_id is required for Core-type position")
            item = (await db.execute(
                select(CoreDataItem).where(
                    CoreDataItem.id == pos.core_data_item_id,
                    CoreDataItem.core_id == schema_pos.core_id,
                    CoreDataItem.status == StatusEnum.ACTIVE
                )
            )).scalar_one_or_none()
            if not item:
                raise HTTPException(
                    status_code=422,
                    detail=f"Position {pos.position_number}: item not found or not active in the expected Core"
                )
            value_id = pos.core_data_item_id
        else:  # CONNECT
            if not pos.connect_data_item_ref_id:
                raise HTTPException(status_code=422, detail=f"Position {pos.position_number}: connect_data_item_ref_id is required for Connect-type position")
            ref_row = (await db.execute(
                select(ConnectDataItem).where(
                    ConnectDataItem.id == pos.connect_data_item_ref_id,
                    ConnectDataItem.connect_id == schema_pos.connect_ref_id,
                    ConnectDataItem.status == StatusEnum.ACTIVE
                )
            )).scalar_one_or_none()
            if not ref_row:
                raise HTTPException(
                    status_code=422,
                    detail=f"Position {pos.position_number}: referenced Connect data row not found or not active"
                )
            value_id = pos.connect_data_item_ref_id

        if value_id in seen_ids:
            raise HTTPException(status_code=422, detail="The same data item cannot appear twice in one Connect Data row")
        seen_ids.append(value_id)
        resolved.append((pos.position_number, value_id))

    return resolved


@router.post("/{connect_id}/items", response_model=ConnectDataItemOut, status_code=status.HTTP_201_CREATED)
async def create_connect_data_item(
    connect_id: str,
    positions: list[ConnectDataPositionIn],
    db: AsyncSession = Depends(get_db),
    current_user=Depends(require_designer_or_stocker),
):
    connect = await get_connect(db, connect_id, current_user)
    check_stocker_exclusive_write(connect.assigned_stocker_id, current_user)

    schema_result = await db.execute(
        select(ConnectSchemaPosition)
        .where(ConnectSchemaPosition.connect_id == connect_id)
        .order_by(ConnectSchemaPosition.position_number)
    )
    schema_positions = schema_result.scalars().all()

    if not schema_positions:
        raise HTTPException(status_code=422, detail="Define the Connect schema before adding data")

    resolved = await _validate_positions(db, positions, schema_positions, connect_id)

    # Cross-row duplicate check (all statuses)
    new_fingerprint = _make_input_fingerprint(positions)
    existing_items = (await db.execute(
        select(ConnectDataItem)
        .options(selectinload(ConnectDataItem.positions))
        .where(ConnectDataItem.connect_id == connect_id)
    )).scalars().all()
    for existing in existing_items:
        if _make_fingerprint(existing.positions) == new_fingerprint:
            msg = (
                "This combination already exists in this Connect (currently inactive — reactivate it instead)"
                if existing.status == StatusEnum.INACTIVE
                else "This combination already exists in this Connect"
            )
            raise HTTPException(status_code=409, detail=msg)

    cdi = ConnectDataItem(connect_id=connect_id, status=StatusEnum.ACTIVE, created_by=current_user.id)
    db.add(cdi)
    await db.flush()

    for pos in positions:
        db.add(ConnectDataPosition(
            connect_data_item_id=cdi.id,
            position_number=pos.position_number,
            core_data_item_id=pos.core_data_item_id,
            connect_data_item_ref_id=pos.connect_data_item_ref_id,
        ))

    try:
        create_neo4j_relationships(cdi.id, connect_id, resolved, schema_positions)
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Neo4J write failed: {str(e)}")

    if not connect.schema_finalised:
        connect.schema_finalised = True

    await write_sync_changes(db, EntityType.CONNECT_DATA_ITEM, cdi.id, ChangeType.ADDED, connect_id=connect_id)
    await db.commit()

    result = await db.execute(
        select(ConnectDataItem)
        .options(selectinload(ConnectDataItem.positions))
        .where(ConnectDataItem.id == cdi.id)
    )
    return result.scalar_one()


# ── Connect Data Edit ─────────────────────────────────────────────────────────

@router.put("/{connect_id}/items/{cdi_id}", response_model=ConnectDataItemOut)
async def update_connect_data_item(
    connect_id: str,
    cdi_id: str,
    positions: list[ConnectDataPositionIn],
    db: AsyncSession = Depends(get_db),
    current_user=Depends(require_designer_or_stocker),
):
    connect = await get_connect(db, connect_id, current_user)
    check_stocker_exclusive_write(connect.assigned_stocker_id, current_user)

    cdi = (await db.execute(
        select(ConnectDataItem)
        .options(selectinload(ConnectDataItem.positions))
        .where(ConnectDataItem.id == cdi_id, ConnectDataItem.connect_id == connect_id)
    )).scalar_one_or_none()
    if not cdi:
        raise HTTPException(status_code=404, detail="Connect Data Item not found")
    if cdi.status != StatusEnum.ACTIVE:
        raise HTTPException(status_code=422, detail="Cannot edit an inactive data row")

    schema_result = await db.execute(
        select(ConnectSchemaPosition)
        .where(ConnectSchemaPosition.connect_id == connect_id)
        .order_by(ConnectSchemaPosition.position_number)
    )
    schema_positions = schema_result.scalars().all()

    resolved = await _validate_positions(db, positions, schema_positions, connect_id)

    # Duplicate check — exclude this row itself
    new_fingerprint = _make_input_fingerprint(positions)
    other_items = (await db.execute(
        select(ConnectDataItem)
        .options(selectinload(ConnectDataItem.positions))
        .where(ConnectDataItem.connect_id == connect_id, ConnectDataItem.id != cdi_id)
    )).scalars().all()
    for other in other_items:
        if _make_fingerprint(other.positions) == new_fingerprint:
            msg = (
                "This combination already exists (currently inactive — reactivate it instead)"
                if other.status == StatusEnum.INACTIVE
                else "This combination already exists in this Connect"
            )
            raise HTTPException(status_code=409, detail=msg)

    for old_pos in cdi.positions:
        await db.delete(old_pos)
    await db.flush()

    for pos in positions:
        db.add(ConnectDataPosition(
            connect_data_item_id=cdi.id,
            position_number=pos.position_number,
            core_data_item_id=pos.core_data_item_id,
            connect_data_item_ref_id=pos.connect_data_item_ref_id,
        ))

    inactivate_neo4j_relationships(cdi_id)
    try:
        create_neo4j_relationships(cdi.id, connect_id, resolved, schema_positions)
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Neo4J write failed: {str(e)}")

    await write_sync_changes(db, EntityType.CONNECT_DATA_ITEM, cdi.id, ChangeType.UPDATED, connect_id=connect_id)
    await db.commit()

    result = await db.execute(
        select(ConnectDataItem)
        .options(selectinload(ConnectDataItem.positions))
        .where(ConnectDataItem.id == cdi.id)
    )
    return result.scalar_one()


# ── Connect Data Status ────────────────────────────────────────────────────────

@router.put("/{connect_id}/items/{cdi_id}/status", response_model=ConnectDataItemOut)
async def update_connect_data_status(
    connect_id: str,
    cdi_id: str,
    request: ConnectDataStatusUpdate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(require_designer_or_stocker),
):
    await get_connect(db, connect_id, current_user)
    result = await db.execute(
        select(ConnectDataItem)
        .options(selectinload(ConnectDataItem.positions))
        .where(ConnectDataItem.id == cdi_id, ConnectDataItem.connect_id == connect_id)
    )
    cdi = result.scalar_one_or_none()
    if not cdi:
        raise HTTPException(status_code=404, detail="Connect Data Item not found")

    cdi.status = request.status
    if request.status == StatusEnum.INACTIVE:
        inactivate_neo4j_relationships(cdi_id)
        change = ChangeType.INACTIVATED
    else:
        change = ChangeType.REACTIVATED

    await write_sync_changes(db, EntityType.CONNECT_DATA_ITEM, cdi_id, change, connect_id=connect_id)
    await db.commit()
    await db.refresh(cdi)
    return cdi


# ── Excel Upload (BL-C-04) ─────────────────────────────────────────────────────

@router.post("/{connect_id}/items/upload-excel", response_model=ExcelUploadReport)
async def upload_excel(
    connect_id: str,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(require_designer_or_stocker),
):
    connect = await get_connect(db, connect_id, current_user)
    check_stocker_exclusive_write(connect.assigned_stocker_id, current_user)

    schema_result = await db.execute(
        select(ConnectSchemaPosition)
        .where(ConnectSchemaPosition.connect_id == connect_id)
        .order_by(ConnectSchemaPosition.position_number)
    )
    schema_positions = schema_result.scalars().all()

    if not schema_positions:
        raise HTTPException(status_code=422, detail="Define the Connect schema before uploading data")

    content = await file.read()
    filename = (file.filename or "").lower()
    is_csv = filename.endswith(".csv")

    if is_csv:
        import csv as _csv
        text = content.decode("utf-8-sig", errors="replace")
        reader = _csv.reader(io.StringIO(text))
        rows = list(reader)
    else:
        try:
            import openpyxl
        except ImportError:
            raise HTTPException(status_code=500, detail="openpyxl is required for Excel upload. Run: pip install openpyxl")
        wb = openpyxl.load_workbook(io.BytesIO(content), data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))

    if not rows:
        raise HTTPException(status_code=422, detail="Uploaded file is empty")

    headers = [str(h).strip() if h else "" for h in rows[0]]
    data_rows = rows[1:]

    # Build column names per position. Position-label wins; otherwise the Core
    # (or Connect) name. When the same fallback name appears multiple times in
    # one schema, both labels and the upload resolution treat the second/third
    # occurrence as "Name (2)", "Name (3)" — keeps duplicate-Core schemas
    # disambiguated even without an explicit position_label.
    raw_names: list[str] = []
    for sp in schema_positions:
        node_type = sp.node_type.value if hasattr(sp.node_type, 'value') else str(sp.node_type)
        if sp.position_label and sp.position_label.strip():
            raw_names.append(sp.position_label.strip())
        elif node_type == 'CORE' and sp.core_id:
            core = (await db.execute(select(Core).where(Core.id == sp.core_id))).scalar_one_or_none()
            raw_names.append(core.name if core else sp.core_id)
        elif sp.connect_ref_id:
            ref_connect = (await db.execute(select(Connect).where(Connect.id == sp.connect_ref_id))).scalar_one_or_none()
            raw_names.append(ref_connect.name if ref_connect else sp.connect_ref_id)
        else:
            raw_names.append(f"Position {sp.position_number}")

    seen: dict[str, int] = {}
    position_names: list[str] = []
    for name in raw_names:
        n = seen.get(name, 0)
        seen[name] = n + 1
        position_names.append(name if n == 0 else f"{name} ({n + 1})")

    resolved_count = 0
    unresolved_count = 0
    skipped_duplicates = 0
    unresolved_details = []

    def _parse_xlsx_datetime(val):
        """Parse creator timestamp from Excel — handles string ISO and Excel datetime objects."""
        if val is None:
            return None
        from datetime import datetime as dt, timezone as tz
        if isinstance(val, dt):
            return val.replace(tzinfo=tz.utc) if val.tzinfo is None else val
        val = str(val).strip()
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
                    "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M:%S"):
            try:
                return dt.strptime(val, fmt).replace(tzinfo=tz.utc)
            except ValueError:
                continue
        return None

    def _get_col(headers, row_values, *names):
        """Read a cell from a row by trying multiple column name variants."""
        for name in names:
            try:
                idx = next(i for i, h in enumerate(headers) if h.lower() == name.lower())
                return row_values[idx] if idx < len(row_values) else None
            except StopIteration:
                continue
        return None

    existing_items = (await db.execute(
        select(ConnectDataItem)
        .options(selectinload(ConnectDataItem.positions))
        .where(ConnectDataItem.connect_id == connect_id, ConnectDataItem.status == StatusEnum.ACTIVE)
    )).scalars().all()
    existing_fingerprints = {_make_fingerprint(item.positions) for item in existing_items}

    # Pre-fetch every Core's items into a per-position lookup dict so the row loop
    # never hits the database for resolution. With 4 positions × 7000 rows the
    # naive path was 28k SELECTs and pushed past nginx's 504 timeout.
    core_lookup_by_pos: dict[int, dict[str, str]] = {}
    for sp in schema_positions:
        node_type = sp.node_type.value if hasattr(sp.node_type, 'value') else str(sp.node_type)
        if node_type == 'CORE' and sp.core_id:
            rows_q = (await db.execute(
                select(CoreDataItem.id, CoreDataItem.english_value).where(
                    CoreDataItem.core_id == sp.core_id,
                    CoreDataItem.status == StatusEnum.ACTIVE,
                )
            )).all()
            core_lookup_by_pos[sp.position_number] = {r.english_value: r.id for r in rows_q}

    schema_map_by_pos = {sp.position_number: sp for sp in schema_positions}

    # Tagged products: fetch once so we can buffer SyncChangeLog rows in-memory.
    tagged_product_ids = (await db.execute(
        select(ConnectProductTag.product_id).where(ConnectProductTag.connect_id == connect_id)
    )).scalars().all()

    from app.models.models import utcnow as _utcnow

    new_cdi_ids: list[str] = []
    neo4j_rels_by_type: dict[str, list[dict]] = {}
    # Collect rows for bulk INSERT at the end instead of db.add()-per-row, which
    # otherwise generates one Postgres round-trip per row at commit time and
    # makes big files (>10k rows) breach the nginx 300s timeout.
    cdi_rows: list[dict] = []
    cdp_rows: list[dict] = []

    for row_num, row in enumerate(data_rows, start=2):
        row_values = [str(v).strip() if v is not None else "" for v in row]
        if all(v == "" for v in row_values):
            continue

        resolved_positions = []
        row_failed = False
        row_errors = []

        for i, sp in enumerate(schema_positions):
            col_name = position_names[i]
            try:
                col_idx = headers.index(col_name)
                value = row_values[col_idx] if col_idx < len(row_values) else ""
            except ValueError:
                value = row_values[i] if i < len(row_values) else ""

            if not value:
                row_errors.append(f"position {sp.position_number}: empty value")
                row_failed = True
                continue

            value = value.removeprefix("ID_").removesuffix("|")
            node_type = sp.node_type.value if hasattr(sp.node_type, 'value') else str(sp.node_type)

            if node_type == 'CORE':
                item_id = core_lookup_by_pos.get(sp.position_number, {}).get(value)
                if not item_id:
                    row_errors.append(f"position {sp.position_number}: '{value}' not found in Core '{col_name}'")
                    row_failed = True
                else:
                    resolved_positions.append((sp.position_number, item_id, None))
            else:
                row_errors.append(f"position {sp.position_number}: Excel upload for Connect-type positions is not yet supported — use manual entry")
                row_failed = True

        if row_failed:
            unresolved_count += 1
            unresolved_details.append({"row": row_num, "errors": row_errors})
            continue

        row_fingerprint = "|".join(
            f"{pos_num}:{item_id}"
            for pos_num, item_id, _ in sorted(resolved_positions, key=lambda x: x[0])
        )
        if row_fingerprint in existing_fingerprints:
            skipped_duplicates += 1
            continue
        existing_fingerprints.add(row_fingerprint)

        csv_creator = _get_col(headers, row_values, "Created By", "created_by", "Created by")
        csv_creator_name = str(csv_creator).strip() if csv_creator and str(csv_creator).strip() not in ("", "---") else None
        csv_ts = _parse_xlsx_datetime(_get_col(headers, row_values, "Created at", "created_at", "Created At"))

        new_id = str(uuid.uuid4())
        cdi_rows.append({
            "id": new_id,
            "connect_id": connect_id,
            "status": StatusEnum.ACTIVE,
            "created_by": current_user.id,
            "legacy_created_by_name": csv_creator_name,
            "created_at": csv_ts or _utcnow(),
        })

        for pos_num, item_id, _ in resolved_positions:
            cdp_rows.append({
                "connect_data_item_id": new_id,
                "position_number": pos_num,
                "core_data_item_id": item_id,
                "connect_data_item_ref_id": None,
            })

        sorted_pos = sorted(resolved_positions, key=lambda x: x[0])
        for j in range(len(sorted_pos) - 1):
            from_pos_num, from_item_id, _ = sorted_pos[j]
            to_pos_num, to_item_id, _ = sorted_pos[j + 1]
            from_schema = schema_map_by_pos[from_pos_num]
            to_schema = schema_map_by_pos[to_pos_num]
            from_type = from_schema.node_type.value if hasattr(from_schema.node_type, 'value') else str(from_schema.node_type)
            to_type = to_schema.node_type.value if hasattr(to_schema.node_type, 'value') else str(to_schema.node_type)
            if from_type == 'CONNECT' or to_type == 'CONNECT':
                continue
            rel_type = from_schema.relationship_type_to_next
            neo4j_rels_by_type.setdefault(rel_type, []).append({
                "from_id": from_item_id,
                "to_id": to_item_id,
                "cdi_id": new_id,
                "from_pos": from_pos_num,
                "to_pos": to_pos_num,
            })

        new_cdi_ids.append(new_id)
        resolved_count += 1

        if not connect.schema_finalised:
            connect.schema_finalised = True

    # Bulk INSERT in chunks so Postgres sees a few large statements rather
    # than one round-trip per row. 5000 rows per statement is a comfortable
    # middle ground for the asyncpg driver — fast and well under the per-
    # statement parameter limit (~32k bind params at 6 cols * 5k rows).
    INSERT_CHUNK = 5000

    if cdi_rows:
        for i in range(0, len(cdi_rows), INSERT_CHUNK):
            await db.execute(insert(ConnectDataItem), cdi_rows[i:i + INSERT_CHUNK])

    if cdp_rows:
        for i in range(0, len(cdp_rows), INSERT_CHUNK):
            await db.execute(insert(ConnectDataPosition), cdp_rows[i:i + INSERT_CHUNK])

    # One Neo4J round trip per relationship type, instead of per row.
    if neo4j_rels_by_type:
        try:
            with _neo_driver.session() as neo_session:
                for rel_type, batch in neo4j_rels_by_type.items():
                    neo_session.run(
                        f"""
                        UNWIND $rels AS row
                        MATCH (a:CoreDataItem {{id: row.from_id}})
                        MATCH (b:CoreDataItem {{id: row.to_id}})
                        CREATE (a)-[:{rel_type} {{
                            connect_data_item_id: row.cdi_id,
                            connect_id: $connect_id,
                            schema_position_from: row.from_pos,
                            schema_position_to: row.to_pos,
                            status: 'ACTIVE'
                        }}]->(b)
                        """,
                        rels=batch,
                        connect_id=connect_id,
                    )
        except Exception as e:
            await db.rollback()
            raise HTTPException(status_code=500, detail=f"Neo4J batch create failed: {str(e)}")

    if new_cdi_ids and tagged_product_ids:
        scl_rows = [
            {
                "product_id": pid,
                "entity_type": EntityType.CONNECT_DATA_ITEM,
                "entity_id": cdi_id,
                "change_type": ChangeType.ADDED,
            }
            for cdi_id in new_cdi_ids
            for pid in tagged_product_ids
        ]
        for i in range(0, len(scl_rows), INSERT_CHUNK):
            await db.execute(insert(SyncChangeLog), scl_rows[i:i + INSERT_CHUNK])

    await db.commit()

    return ExcelUploadReport(
        total_rows=len(data_rows),
        resolved=resolved_count,
        unresolved=unresolved_count,
        skipped_duplicates=skipped_duplicates,
        unresolved_details=unresolved_details,
    )


# ── Duplicate detection & cleanup ─────────────────────────────────────────────
# Identifies ConnectDataItems that share the same position fingerprint
# (the same set of (position_number, core_data_item_id) tuples). Created to
# clean up the doubling caused by 504-then-retry uploads where the api process
# kept committing in the background after nginx had given up.

async def _build_position_labels(db: AsyncSession, schema_positions) -> dict[int, str]:
    raw_names: list[tuple[int, str]] = []
    for sp in schema_positions:
        node_type = sp.node_type.value if hasattr(sp.node_type, 'value') else str(sp.node_type)
        if sp.position_label and sp.position_label.strip():
            raw_names.append((sp.position_number, sp.position_label.strip()))
        elif node_type == 'CORE' and sp.core_id:
            core = (await db.execute(select(Core).where(Core.id == sp.core_id))).scalar_one_or_none()
            raw_names.append((sp.position_number, core.name if core else f"Position {sp.position_number}"))
        elif sp.connect_ref_id:
            ref_connect = (await db.execute(select(Connect).where(Connect.id == sp.connect_ref_id))).scalar_one_or_none()
            raw_names.append((sp.position_number, ref_connect.name if ref_connect else f"Position {sp.position_number}"))
        else:
            raw_names.append((sp.position_number, f"Position {sp.position_number}"))

    seen: dict[str, int] = {}
    out: dict[int, str] = {}
    for pn, name in raw_names:
        n = seen.get(name, 0)
        seen[name] = n + 1
        out[pn] = name if n == 0 else f"{name} ({n + 1})"
    return out


@router.get("/{connect_id}/duplicates", response_model=DuplicatesResponse)
async def list_duplicates(
    connect_id: str,
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(require_designer_or_stocker),
):
    """List groups of ACTIVE ConnectDataItems that share the same position
    fingerprint. Within each group, rows are returned oldest-first so the UI
    can mark the first as the keeper."""
    await get_connect(db, connect_id, current_user)

    items = (await db.execute(
        select(ConnectDataItem)
        .options(selectinload(ConnectDataItem.positions))
        .where(ConnectDataItem.connect_id == connect_id, ConnectDataItem.status == StatusEnum.ACTIVE)
    )).scalars().all()

    groups: dict[str, list[ConnectDataItem]] = {}
    for item in items:
        fp = _make_fingerprint(item.positions)
        groups.setdefault(fp, []).append(item)

    dup_groups = [(fp, members) for fp, members in groups.items() if len(members) > 1]
    dup_groups.sort(key=lambda g: (-len(g[1]), min(m.created_at for m in g[1])))

    total_groups = len(dup_groups)
    total_extra = sum(len(members) - 1 for _, members in dup_groups)

    page = dup_groups[skip:skip + limit]

    core_item_ids: set[str] = set()
    for _, members in page:
        for m in members:
            for pos in m.positions:
                if pos.core_data_item_id:
                    core_item_ids.add(pos.core_data_item_id)

    core_items_map: dict[str, str] = {}
    if core_item_ids:
        rows_q = (await db.execute(
            select(CoreDataItem.id, CoreDataItem.english_value)
            .where(CoreDataItem.id.in_(list(core_item_ids)))
        )).all()
        core_items_map = {r.id: r.english_value for r in rows_q}

    schema_positions = (await db.execute(
        select(ConnectSchemaPosition)
        .where(ConnectSchemaPosition.connect_id == connect_id)
        .order_by(ConnectSchemaPosition.position_number)
    )).scalars().all()
    pos_labels = await _build_position_labels(db, schema_positions)

    out_groups: list[DuplicateGroup] = []
    for fp, members in page:
        members_sorted = sorted(members, key=lambda m: m.created_at)
        out_rows: list[DuplicateRow] = []
        for m in members_sorted:
            pvs: list[DuplicatePositionValue] = []
            for pos in sorted(m.positions, key=lambda p: p.position_number):
                if pos.core_data_item_id:
                    value = core_items_map.get(pos.core_data_item_id, "—")
                elif pos.connect_data_item_ref_id:
                    value = f"[Connect ref] {pos.connect_data_item_ref_id[:8]}"
                else:
                    value = "—"
                pvs.append(DuplicatePositionValue(
                    position_number=pos.position_number,
                    label=pos_labels.get(pos.position_number, f"Position {pos.position_number}"),
                    value=value,
                ))
            out_rows.append(DuplicateRow(
                cdi_id=m.id,
                created_at=m.created_at.isoformat() if m.created_at else None,
                legacy_created_by_name=m.legacy_created_by_name,
                position_values=pvs,
            ))
        out_groups.append(DuplicateGroup(
            fingerprint=fp,
            count=len(members),
            rows=out_rows,
        ))

    return DuplicatesResponse(
        total_groups=total_groups,
        total_extra_items=total_extra,
        skip=skip,
        limit=limit,
        groups=out_groups,
    )


@router.post("/{connect_id}/duplicates/cleanup", response_model=DuplicateCleanupResponse)
async def cleanup_duplicates(
    connect_id: str,
    body: DuplicateCleanupRequest,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(require_designer),
):
    """Inactivate duplicate ConnectDataItems, keeping the oldest of each group.
    Sets status to INACTIVE in Postgres, flips Neo4J relationships to INACTIVE,
    and emits sync_change_log rows for each tagged product. No physical delete."""
    await get_connect(db, connect_id, current_user)

    if not body.fingerprint and not body.all:
        raise HTTPException(status_code=422, detail="Provide fingerprint or set all=true")

    items = (await db.execute(
        select(ConnectDataItem)
        .options(selectinload(ConnectDataItem.positions))
        .where(ConnectDataItem.connect_id == connect_id, ConnectDataItem.status == StatusEnum.ACTIVE)
    )).scalars().all()

    groups: dict[str, list[ConnectDataItem]] = {}
    for item in items:
        fp = _make_fingerprint(item.positions)
        groups.setdefault(fp, []).append(item)

    if body.all:
        target_groups = [g for g in groups.values() if len(g) > 1]
    else:
        if body.fingerprint not in groups:
            raise HTTPException(status_code=404, detail="Fingerprint not found")
        target_groups = [groups[body.fingerprint]]

    all_ids: list[str] = []
    for grp in target_groups:
        if len(grp) <= 1:
            continue
        sorted_grp = sorted(grp, key=lambda m: m.created_at)
        for member in sorted_grp[1:]:
            all_ids.append(member.id)

    if not all_ids:
        return DuplicateCleanupResponse(
            groups_processed=0, items_inactivated=0,
            has_more=False, remaining=0,
        )

    # Chunk so each request finishes inside nginx's timeout. Commit after every
    # chunk so partial progress always survives — repeated calls are idempotent
    # because already-INACTIVE rows are filtered out at the top of this handler.
    LIMIT_PER_CALL = 2000
    BATCH_SIZE = 200

    to_process = all_ids[:LIMIT_PER_CALL]
    remaining = max(0, len(all_ids) - len(to_process))

    tagged_pids = (await db.execute(
        select(ConnectProductTag.product_id).where(ConnectProductTag.connect_id == connect_id)
    )).scalars().all()

    items_inactivated = 0
    for chunk_start in range(0, len(to_process), BATCH_SIZE):
        chunk = to_process[chunk_start:chunk_start + BATCH_SIZE]

        await db.execute(
            update(ConnectDataItem)
            .where(ConnectDataItem.id.in_(chunk))
            .values(status=StatusEnum.INACTIVE)
        )

        try:
            with _neo_driver.session() as neo:
                # Single relationship scan + IN filter — much faster than
                # UNWIND-then-MATCH per id when there is no index on
                # r.connect_data_item_id.
                neo.run(
                    """
                    MATCH ()-[r]-()
                    WHERE r.connect_data_item_id IN $ids
                    SET r.status = 'INACTIVE'
                    """,
                    ids=chunk,
                )
        except Exception as e:
            await db.rollback()
            raise HTTPException(status_code=500, detail=f"Neo4J batch inactivate failed at chunk {chunk_start}: {str(e)}")

        for cdi_id in chunk:
            for pid in tagged_pids:
                db.add(SyncChangeLog(
                    product_id=pid,
                    entity_type=EntityType.CONNECT_DATA_ITEM,
                    entity_id=cdi_id,
                    change_type=ChangeType.INACTIVATED,
                ))

        await db.commit()
        items_inactivated += len(chunk)

    # Each duplicate group has count >= 2; in this Connect every group is a
    # 2-row pair so groups_processed == items_inactivated. For groups with
    # higher count, it's an approximation — the caller should rely on
    # items_inactivated for the authoritative number.
    return DuplicateCleanupResponse(
        groups_processed=items_inactivated,
        items_inactivated=items_inactivated,
        has_more=remaining > 0,
        remaining=remaining,
    )
