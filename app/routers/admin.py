"""
Admin utility endpoints — migration status, public visibility, health checks.
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text as sql_text
from app.database import get_db
from app.dependencies import require_role
from app.models.models import (
    UserRole, Core, CoreDataItem, CoreDataTranslation, Connect, ConnectDataItem,
    SimilarityPair, SimilarityStatus, CoreType, StatusEnum, LanguageRegistry,
    RelationshipTypeRegistry, ProductRegistry, ConnectSchemaPosition,
)
from app.neo4j_db import driver

router = APIRouter(prefix="/admin", tags=["Admin"])
require_admin = require_role(UserRole.ADMIN)


@router.get("/migration/status")
async def migration_status(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
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
    return [
        {"id": r.id, "label": r.label, "display_name": r.display_name,
         "description": r.description, "example": r.example}
        for r in result.scalars().all()
    ]


@router.get("/registries/products")
async def list_products(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ProductRegistry).order_by(ProductRegistry.display_name))
    return [
        {"id": p.id, "name": p.name, "display_name": p.display_name,
         "sync_endpoint_url": p.sync_endpoint_url, "status": p.status.value}
        for p in result.scalars().all()
    ]
