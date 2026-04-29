"""
Admin utility endpoints — migration status, public visibility, health checks.
"""
from fastapi import APIRouter, Depends, HTTPException, status as http_status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update, text as sql_text
from typing import Optional
from pydantic import BaseModel
from app.database import get_db
from app.dependencies import require_role
from app.models.models import (
    UserRole, Core, CoreDataItem, CoreDataTranslation, Connect, ConnectDataItem,
    SimilarityPair, SimilarityStatus, CoreType, StatusEnum, LanguageRegistry,
    RelationshipTypeRegistry, ProductRegistry, ConnectSchemaPosition, User,
)
from app.neo4j_db import driver


class RelTypeCreate(BaseModel):
    label: str
    display_name: str
    description: Optional[str] = None
    example: Optional[str] = None


class RelTypeUpdate(BaseModel):
    label: Optional[str] = None
    display_name: Optional[str] = None
    description: Optional[str] = None
    example: Optional[str] = None


class ProductCreate(BaseModel):
    name: str                                    # machine identifier: rootstalk, pestest
    display_name: str                            # human label: RootsTalk
    sync_endpoint_url: Optional[str] = None     # URL Cosh POSTs payload to
    sync_api_key: Optional[str] = None          # shared secret sent in Authorization header


class ProductUpdate(BaseModel):
    display_name: Optional[str] = None
    sync_endpoint_url: Optional[str] = None
    sync_api_key: Optional[str] = None
    status: Optional[StatusEnum] = None

router = APIRouter(prefix="/admin", tags=["Admin"])
require_admin = require_role(UserRole.ADMIN)
require_designer_or_admin = require_role(UserRole.DESIGNER, UserRole.ADMIN)


@router.get("/migration/status")
async def migration_status(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_designer_or_admin),
):
    """
    P7-04: Live migration status dashboard.
    Returns counts from PostgreSQL and Neo4J for quick verification.
    """
    # Core counts
    core_rows = (await db.execute(
        select(Core.name, Core.core_type, func.count(CoreDataItem.id).label("item_count"))
        .outerjoin(CoreDataItem, (CoreDataItem.core_id == Core.id) & (CoreDataItem.status == StatusEnum.ACTIVE))
        .where(Core.status == StatusEnum.ACTIVE)
        .group_by(Core.id, Core.name, Core.core_type)
        .order_by(Core.name)
    )).all()

    total_core_items = sum(r.item_count for r in core_rows)

    # Connect counts
    connect_rows = (await db.execute(
        select(Connect.name, func.count(ConnectDataItem.id).label("item_count"))
        .outerjoin(ConnectDataItem, (ConnectDataItem.connect_id == Connect.id) & (ConnectDataItem.status == StatusEnum.ACTIVE))
        .where(Connect.status == StatusEnum.ACTIVE)
        .group_by(Connect.id, Connect.name)
        .order_by(Connect.name)
    )).all()

    total_connect_items = sum(r.item_count for r in connect_rows)

    # Translation coverage
    languages = (await db.execute(
        select(LanguageRegistry.language_code, LanguageRegistry.language_name_en)
        .where(LanguageRegistry.status == StatusEnum.ACTIVE, LanguageRegistry.language_code != "en")
        .order_by(LanguageRegistry.language_code)
    )).all()

    text_item_count = (await db.execute(
        select(func.count(CoreDataItem.id))
        .join(Core, Core.id == CoreDataItem.core_id)
        .where(CoreDataItem.status == StatusEnum.ACTIVE, Core.core_type == CoreType.TEXT)
    )).scalar_one()

    translation_coverage = []
    for lang in languages:
        translated = (await db.execute(
            select(func.count(CoreDataTranslation.id))
            .where(CoreDataTranslation.language_code == lang.language_code)
        )).scalar_one()
        expert = (await db.execute(
            select(func.count(CoreDataTranslation.id))
            .where(
                CoreDataTranslation.language_code == lang.language_code,
                CoreDataTranslation.validation_status == "EXPERT_VALIDATED",
            )
        )).scalar_one()
        pct = round(translated / text_item_count * 100, 1) if text_item_count else 0
        translation_coverage.append({
            "language_code": lang.language_code,
            "language_name": lang.language_name_en,
            "translated": translated,
            "expert_validated": expert,
            "coverage_pct": pct,
        })

    # Similarity pairs
    sim_rows = (await db.execute(
        select(SimilarityPair.status, func.count(SimilarityPair.id).label("cnt"))
        .group_by(SimilarityPair.status)
    )).all()
    similarity_summary = {r.status.value: r.cnt for r in sim_rows}

    # Neo4J counts
    neo4j_status = {}
    try:
        with driver.session() as neo_session:
            node_count = neo_session.run(
                "MATCH (n:CoreDataItem) RETURN count(n) AS cnt"
            ).single()["cnt"]
            active_node_count = neo_session.run(
                "MATCH (n:CoreDataItem {status: 'ACTIVE'}) RETURN count(n) AS cnt"
            ).single()["cnt"]
            rel_count = neo_session.run(
                "MATCH ()-[r]->() RETURN count(r) AS cnt"
            ).single()["cnt"]
            neo4j_status = {
                "total_nodes": node_count,
                "active_nodes": active_node_count,
                "total_relationships": rel_count,
                "pg_neo4j_match": active_node_count == total_core_items,
            }
    except Exception as e:
        neo4j_status = {"error": str(e)}

    return {
        "postgresql": {
            "cores": [
                {"name": r.name, "type": r.core_type.value, "active_items": r.item_count}
                for r in core_rows
            ],
            "total_core_data_items": total_core_items,
            "connects": [
                {"name": r.name, "active_items": r.item_count}
                for r in connect_rows
            ],
            "total_connect_data_items": total_connect_items,
        },
        "translations": {
            "text_core_items": text_item_count,
            "coverage_by_language": translation_coverage,
        },
        "neo4j": neo4j_status,
        "similarity": similarity_summary,
        "migration_ready": (
            total_core_items > 0
            and neo4j_status.get("pg_neo4j_match", False)
        ),
    }


# ── Public Visibility (P8-01) ──────────────────────────────────────────────────

@router.get("/public-entities")
async def list_public_entities(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """List all Cores and Connects currently marked as publicly visible."""
    public_cores = (await db.execute(
        select(Core.id, Core.name, Core.folder_id)
        .where(Core.is_public == True, Core.status == StatusEnum.ACTIVE)
        .order_by(Core.name)
    )).all()

    public_connects = (await db.execute(
        select(Connect.id, Connect.name)
        .where(Connect.is_public == True, Connect.status == StatusEnum.ACTIVE)
        .order_by(Connect.name)
    )).all()

    return {
        "public_cores": [{"id": r.id, "name": r.name, "folder_id": r.folder_id} for r in public_cores],
        "public_connects": [{"id": r.id, "name": r.name} for r in public_connects],
    }


@router.put("/cores/{core_id}/visibility")
async def set_core_visibility(
    core_id: str,
    is_public: bool,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Mark a Core as publicly visible (or private) on the knowledge graph."""
    core = (await db.execute(select(Core).where(Core.id == core_id))).scalar_one_or_none()
    if not core:
        raise HTTPException(status_code=404, detail="Core not found")
    core.is_public = is_public
    await db.commit()
    return {"id": core.id, "name": core.name, "is_public": core.is_public}


@router.put("/connects/{connect_id}/visibility")
async def set_connect_visibility(
    connect_id: str,
    is_public: bool,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Mark a Connect as publicly visible (or private) on the knowledge graph."""
    connect = (await db.execute(select(Connect).where(Connect.id == connect_id))).scalar_one_or_none()
    if not connect:
        raise HTTPException(status_code=404, detail="Connect not found")
    connect.is_public = is_public
    await db.commit()
    return {"id": connect.id, "name": connect.name, "is_public": connect.is_public}


# ── Registry read endpoints (for frontend) ────────────────────────────────────

@router.get("/registries/languages")
async def list_languages(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(LanguageRegistry).order_by(LanguageRegistry.language_name_en))
    return [
        {"id": l.id, "language_code": l.language_code, "language_name_en": l.language_name_en,
         "language_name_native": l.language_name_native, "script": l.script,
         "direction": l.direction.value, "status": l.status.value}
        for l in result.scalars().all()
    ]


@router.get("/registries/relationship-types")
async def list_relationship_types(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(RelationshipTypeRegistry).order_by(RelationshipTypeRegistry.label))
    rts = result.scalars().all()

    # Count how many schema positions use each label
    usage_rows = (await db.execute(
        select(ConnectSchemaPosition.relationship_type_to_next, func.count(ConnectSchemaPosition.id).label("cnt"))
        .where(ConnectSchemaPosition.relationship_type_to_next.isnot(None))
        .group_by(ConnectSchemaPosition.relationship_type_to_next)
    )).all()
    usage_map = {r.relationship_type_to_next: r.cnt for r in usage_rows}

    return [
        {"id": r.id, "label": r.label, "display_name": r.display_name,
         "description": r.description, "example": r.example,
         "usage_count": usage_map.get(r.label, 0)}
        for r in rts
    ]


@router.post("/registries/relationship-types", status_code=http_status.HTTP_201_CREATED)
async def create_relationship_type(
    request: RelTypeCreate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(require_admin),
):
    existing = (await db.execute(
        select(RelationshipTypeRegistry).where(RelationshipTypeRegistry.label == request.label)
    )).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail=f"A relationship type with label '{request.label}' already exists")

    rt = RelationshipTypeRegistry(
        label=request.label,
        display_name=request.display_name,
        description=request.description,
        example=request.example,
        added_by=current_user.id,
    )
    db.add(rt)
    await db.commit()
    await db.refresh(rt)
    return {"id": rt.id, "label": rt.label, "display_name": rt.display_name,
            "description": rt.description, "example": rt.example, "usage_count": 0}


@router.put("/registries/relationship-types/{rt_id}")
async def update_relationship_type(
    rt_id: str,
    request: RelTypeUpdate,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    rt = (await db.execute(
        select(RelationshipTypeRegistry).where(RelationshipTypeRegistry.id == rt_id)
    )).scalar_one_or_none()
    if not rt:
        raise HTTPException(status_code=404, detail="Relationship type not found")

    if request.label is not None and request.label != rt.label:
        conflict = (await db.execute(
            select(RelationshipTypeRegistry).where(RelationshipTypeRegistry.label == request.label)
        )).scalar_one_or_none()
        if conflict:
            raise HTTPException(status_code=409, detail=f"Label '{request.label}' is already used by another relationship type")

        # Cascade: update all schema positions that use the old label
        await db.execute(
            update(ConnectSchemaPosition)
            .where(ConnectSchemaPosition.relationship_type_to_next == rt.label)
            .values(relationship_type_to_next=request.label)
        )
        rt.label = request.label

    if request.display_name is not None:
        rt.display_name = request.display_name
    if request.description is not None:
        rt.description = request.description
    if request.example is not None:
        rt.example = request.example

    await db.commit()
    await db.refresh(rt)

    usage_count = (await db.execute(
        select(func.count(ConnectSchemaPosition.id))
        .where(ConnectSchemaPosition.relationship_type_to_next == rt.label)
    )).scalar_one()

    return {"id": rt.id, "label": rt.label, "display_name": rt.display_name,
            "description": rt.description, "example": rt.example, "usage_count": usage_count}


@router.get("/registries/products")
async def list_products(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ProductRegistry).order_by(ProductRegistry.display_name))
    return [
        {
            "id": p.id,
            "name": p.name,
            "display_name": p.display_name,
            "sync_endpoint_url": p.sync_endpoint_url,
            "sync_api_key": p.sync_api_key_secret_name,   # field reused as direct key store
            "status": p.status.value,
        }
        for p in result.scalars().all()
    ]


@router.post("/registries/products", status_code=http_status.HTTP_201_CREATED)
async def create_product(
    request: ProductCreate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(require_admin),
):
    # name must be lowercase, no spaces
    name = request.name.strip().lower().replace(" ", "_")
    existing = (await db.execute(
        select(ProductRegistry).where(ProductRegistry.name == name)
    )).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail=f"A product with identifier '{name}' already exists")

    product = ProductRegistry(
        name=name,
        display_name=request.display_name.strip(),
        sync_endpoint_url=request.sync_endpoint_url or None,
        sync_api_key_secret_name=request.sync_api_key or None,
        status=StatusEnum.ACTIVE,
        added_by=current_user.id,
    )
    db.add(product)
    await db.commit()
    await db.refresh(product)
    return {
        "id": product.id, "name": product.name, "display_name": product.display_name,
        "sync_endpoint_url": product.sync_endpoint_url,
        "sync_api_key": product.sync_api_key_secret_name,
        "status": product.status.value,
    }


@router.put("/registries/products/{product_id}")
async def update_product(
    product_id: str,
    request: ProductUpdate,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    product = (await db.execute(
        select(ProductRegistry).where(ProductRegistry.id == product_id)
    )).scalar_one_or_none()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    if request.display_name is not None:
        product.display_name = request.display_name.strip()
    if request.sync_endpoint_url is not None:
        product.sync_endpoint_url = request.sync_endpoint_url.strip() or None
    if request.sync_api_key is not None:
        product.sync_api_key_secret_name = request.sync_api_key.strip() or None
    if request.status is not None:
        product.status = request.status

    await db.commit()
    await db.refresh(product)
    return {
        "id": product.id, "name": product.name, "display_name": product.display_name,
        "sync_endpoint_url": product.sync_endpoint_url,
        "sync_api_key": product.sync_api_key_secret_name,
        "status": product.status.value,
    }
