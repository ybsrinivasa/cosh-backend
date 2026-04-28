import io
from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from sqlalchemy.orm import selectinload
from app.database import get_db
from app.dependencies import require_role, is_stocker_only
from app.models.models import (
    Connect, ConnectSchemaPosition, ConnectDataItem, ConnectDataPosition,
    ConnectProductTag, ProductRegistry, CoreDataItem, Core,
    UserRole, StatusEnum
)
from app.schemas.connects import (
    ConnectCreate, ConnectUpdate, ConnectOut, SchemaPositionIn, SchemaPositionOut,
    ConnectDataPositionIn, ConnectDataItemOut, ConnectProductTagOut,
    ConnectDataStatusUpdate, ExcelUploadReport
)
from app.services.connect_service import (
    get_connect, check_schema_uniqueness, validate_relationship_type,
    create_neo4j_relationships, inactivate_neo4j_relationships
)
from app.services.sync_service import write_sync_changes
from app.models.models import EntityType, ChangeType

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
    if request.assigned_stocker_id is not None:
        connect.assigned_stocker_id = request.assigned_stocker_id

    await db.commit()
    await db.refresh(connect)
    return connect


# ── Connect Schema ─────────────────────────────────────────────────────────────

@router.get("/{connect_id}/schema", response_model=list[SchemaPositionOut])
async def get_schema(connect_id: str, db: AsyncSession = Depends(get_db), current_user=Depends(require_designer_or_stocker)):
    await get_connect(db, connect_id, current_user)
    result = await db.execute(
        select(ConnectSchemaPosition)
        .where(ConnectSchemaPosition.connect_id == connect_id)
        .order_by(ConnectSchemaPosition.position_number)
    )
    return result.scalars().all()


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
        if is_last:
            if pos.relationship_type_to_next:
                raise HTTPException(status_code=422, detail="The last position must not have a relationship_type_to_next")
        else:
            if not pos.relationship_type_to_next:
                raise HTTPException(status_code=422, detail=f"Position {pos.position_number} must have a relationship_type_to_next")
            await validate_relationship_type(db, pos.relationship_type_to_next)

        core_exists = (await db.execute(select(Core).where(Core.id == pos.core_id))).scalar_one_or_none()
        if not core_exists:
            raise HTTPException(status_code=404, detail=f"Core '{pos.core_id}' not found for position {pos.position_number}")

    position_dicts = [{"position_number": p.position_number, "core_id": p.core_id, "relationship_type_to_next": p.relationship_type_to_next} for p in sorted_positions]
    await check_schema_uniqueness(db, position_dicts, exclude_connect_id=connect_id)

    existing = await db.execute(select(ConnectSchemaPosition).where(ConnectSchemaPosition.connect_id == connect_id))
    for row in existing.scalars().all():
        await db.delete(row)

    new_positions = []
    for pos in sorted_positions:
        schema_pos = ConnectSchemaPosition(
            connect_id=connect_id,
            position_number=pos.position_number,
            core_id=pos.core_id,
            relationship_type_to_next=pos.relationship_type_to_next,
        )
        db.add(schema_pos)
        new_positions.append(schema_pos)

    await db.commit()
    result = await db.execute(
        select(ConnectSchemaPosition)
        .where(ConnectSchemaPosition.connect_id == connect_id)
        .order_by(ConnectSchemaPosition.position_number)
    )
    return result.scalars().all()


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


# ── Connect Data Items — Manual Entry ─────────────────────────────────────────

@router.get("/{connect_id}/items", response_model=list[ConnectDataItemOut])
async def list_connect_data_items(
    connect_id: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(require_designer_or_stocker),
):
    await get_connect(db, connect_id, current_user)
    result = await db.execute(
        select(ConnectDataItem)
        .options(selectinload(ConnectDataItem.positions))
        .where(ConnectDataItem.connect_id == connect_id)
        .order_by(ConnectDataItem.created_at)
    )
    return result.scalars().all()


@router.post("/{connect_id}/items", response_model=ConnectDataItemOut, status_code=status.HTTP_201_CREATED)
async def create_connect_data_item(
    connect_id: str,
    positions: list[ConnectDataPositionIn],
    db: AsyncSession = Depends(get_db),
    current_user=Depends(require_designer_or_stocker),
):
    connect = await get_connect(db, connect_id, current_user)

    schema_result = await db.execute(
        select(ConnectSchemaPosition)
        .where(ConnectSchemaPosition.connect_id == connect_id)
        .order_by(ConnectSchemaPosition.position_number)
    )
    schema_positions = schema_result.scalars().all()

    if not schema_positions:
        raise HTTPException(status_code=422, detail="Define the Connect schema before adding data")

    if len(positions) != len(schema_positions):
        raise HTTPException(status_code=422, detail=f"Expected {len(schema_positions)} positions, got {len(positions)}")

    schema_map = {p.position_number: p for p in schema_positions}
    item_ids_used = []

    for pos in positions:
        if pos.position_number not in schema_map:
            raise HTTPException(status_code=422, detail=f"Position {pos.position_number} not in schema")

        schema_pos = schema_map[pos.position_number]
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
                detail=f"Position {pos.position_number}: item '{pos.core_data_item_id}' not found or not active in the expected Core"
            )

        if pos.core_data_item_id in item_ids_used:
            raise HTTPException(status_code=422, detail="The same data item cannot appear twice in one Connect Data row")
        item_ids_used.append(pos.core_data_item_id)

    cdi = ConnectDataItem(
        connect_id=connect_id,
        status=StatusEnum.ACTIVE,
        created_by=current_user.id,
    )
    db.add(cdi)
    await db.flush()

    for pos in positions:
        db.add(ConnectDataPosition(
            connect_data_item_id=cdi.id,
            position_number=pos.position_number,
            core_data_item_id=pos.core_data_item_id,
        ))

    resolved = [(p.position_number, p.core_data_item_id) for p in positions]
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
    try:
        import openpyxl
    except ImportError:
        raise HTTPException(status_code=500, detail="openpyxl is required for Excel upload. Run: pip install openpyxl")

    connect = await get_connect(db, connect_id, current_user)

    schema_result = await db.execute(
        select(ConnectSchemaPosition)
        .where(ConnectSchemaPosition.connect_id == connect_id)
        .order_by(ConnectSchemaPosition.position_number)
    )
    schema_positions = schema_result.scalars().all()

    if not schema_positions:
        raise HTTPException(status_code=422, detail="Define the Connect schema before uploading data")

    content = await file.read()
    wb = openpyxl.load_workbook(io.BytesIO(content), data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))

    if not rows:
        raise HTTPException(status_code=422, detail="Excel file is empty")

    headers = [str(h).strip() if h else "" for h in rows[0]]
    data_rows = rows[1:]

    core_names = []
    for sp in schema_positions:
        core = (await db.execute(select(Core).where(Core.id == sp.core_id))).scalar_one_or_none()
        core_names.append(core.name if core else sp.core_id)

    resolved_count = 0
    unresolved_count = 0
    unresolved_details = []

    for row_num, row in enumerate(data_rows, start=2):
        row_values = [str(v).strip() if v is not None else "" for v in row]

        if all(v == "" for v in row_values):
            continue

        resolved_positions = []
        row_failed = False
        row_errors = []

        for i, sp in enumerate(schema_positions):
            col_name = core_names[i]
            try:
                col_idx = headers.index(col_name)
                value = row_values[col_idx] if col_idx < len(row_values) else ""
            except ValueError:
                value = row_values[i] if i < len(row_values) else ""

            if not value:
                row_errors.append(f"position {sp.position_number}: empty value")
                row_failed = True
                continue

            value = value.lstrip("ID_").rstrip("|")

            item = (await db.execute(
                select(CoreDataItem).where(
                    CoreDataItem.english_value == value,
                    CoreDataItem.core_id == sp.core_id,
                    CoreDataItem.status == StatusEnum.ACTIVE,
                )
            )).scalar_one_or_none()

            if not item:
                row_errors.append(f"position {sp.position_number}: '{value}' not found in Core '{col_name}'")
                row_failed = True
            else:
                resolved_positions.append((sp.position_number, item.id))

        if row_failed:
            unresolved_count += 1
            unresolved_details.append({"row": row_num, "errors": row_errors})
            continue

        cdi = ConnectDataItem(
            connect_id=connect_id,
            status=StatusEnum.ACTIVE,
            created_by=current_user.id,
        )
        db.add(cdi)
        await db.flush()

        for pos_num, item_id in resolved_positions:
            db.add(ConnectDataPosition(
                connect_data_item_id=cdi.id,
                position_number=pos_num,
                core_data_item_id=item_id,
            ))

        try:
            create_neo4j_relationships(cdi.id, connect_id, resolved_positions, schema_positions)
            resolved_count += 1
        except Exception as e:
            await db.rollback()
            unresolved_count += 1
            unresolved_details.append({"row": row_num, "errors": [f"Neo4J write failed: {str(e)}"]})
            continue

        await write_sync_changes(db, EntityType.CONNECT_DATA_ITEM, cdi.id, ChangeType.ADDED, connect_id=connect_id)

        if not connect.schema_finalised:
            connect.schema_finalised = True

    await db.commit()

    return ExcelUploadReport(
        total_rows=len(data_rows),
        resolved=resolved_count,
        unresolved=unresolved_count,
        unresolved_details=unresolved_details,
    )
