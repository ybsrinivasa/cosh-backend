"""
Connect service — implements BL-C-03 (schema uniqueness) and BL-C-04 (data resolution).
"""
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from fastapi import HTTPException
from app.models.models import (
    Connect, ConnectSchemaPosition, ConnectDataItem, ConnectDataPosition,
    CoreDataItem, RelationshipTypeRegistry, StatusEnum
)
from app.neo4j_db import driver
from app.dependencies import is_stocker_only


async def get_connect(db: AsyncSession, connect_id: str, current_user=None) -> Connect:
    result = await db.execute(select(Connect).where(Connect.id == connect_id))
    connect = result.scalar_one_or_none()
    if not connect:
        raise HTTPException(status_code=404, detail="Connect not found")
    if current_user and is_stocker_only(current_user):
        if connect.assigned_stocker_id != current_user.id:
            raise HTTPException(status_code=403, detail="You are not assigned to this Connect")
    return connect


def _build_fingerprint(positions: list) -> str:
    """BL-C-03: canonical fingerprint for schema uniqueness check."""
    parts = []
    for pos in sorted(positions, key=lambda p: p["position_number"]):
        rel = pos.get("relationship_type_to_next") or "null"
        node_ref = pos.get("connect_ref_id") or pos.get("core_id") or ""
        parts.append(f"{node_ref}:{rel}")
    return "|".join(parts)


async def check_schema_uniqueness(db: AsyncSession, positions: list, exclude_connect_id: str = None):
    """BL-C-03: reject if a structurally identical Connect already exists."""
    new_fingerprint = _build_fingerprint(positions)

    result = await db.execute(
        select(Connect).options(selectinload(Connect.schema_positions))
    )
    existing_connects = result.scalars().all()

    for existing in existing_connects:
        if exclude_connect_id and existing.id == exclude_connect_id:
            continue
        if not existing.schema_positions:
            continue
        existing_fp = _build_fingerprint([
            {"position_number": p.position_number, "core_id": p.core_id, "relationship_type_to_next": p.relationship_type_to_next}
            for p in existing.schema_positions
        ])
        if existing_fp == new_fingerprint:
            raise HTTPException(
                status_code=409,
                detail=f"A structurally identical Connect already exists: '{existing.name}'. Connects are identified by structure, not name."
            )


async def check_schema_uniqueness_with_connect_refs(db: AsyncSession, positions: list, exclude_connect_id: str = None):
    """BL-C-03 variant that handles both Core and Connect-type positions."""
    new_fingerprint = _build_fingerprint(positions)

    result = await db.execute(
        select(Connect).options(selectinload(Connect.schema_positions))
    )
    existing_connects = result.scalars().all()

    for existing in existing_connects:
        if exclude_connect_id and existing.id == exclude_connect_id:
            continue
        if not existing.schema_positions:
            continue
        existing_fp = _build_fingerprint([
            {
                "position_number": p.position_number,
                "core_id": p.core_id,
                "connect_ref_id": p.connect_ref_id,
                "relationship_type_to_next": p.relationship_type_to_next,
            }
            for p in existing.schema_positions
        ])
        if existing_fp == new_fingerprint:
            raise HTTPException(
                status_code=409,
                detail=f"A structurally identical Connect already exists: '{existing.name}'. Connects are identified by structure, not name."
            )


async def validate_relationship_type(db: AsyncSession, label: str):
    """Ensure a relationship type label exists in the registry."""
    result = await db.execute(
        select(RelationshipTypeRegistry).where(RelationshipTypeRegistry.label == label)
    )
    if not result.scalar_one_or_none():
        raise HTTPException(
            status_code=422,
            detail=f"Relationship type '{label}' is not in the Registry. Ask the Admin to add it first."
        )


def create_neo4j_relationships(connect_data_item_id: str, connect_id: str, resolved_positions: list, schema_positions: list):
    """
    BL-C-04: Create N-1 directed relationships in Neo4J for one Connect Data row.
    resolved_positions: list of (position_number, value_id) sorted by position
    schema_positions: list of ConnectSchemaPosition objects
    Pairs where either end is a Connect-type position are skipped — ConnectDataItem
    nodes are not yet indexed in Neo4J; PostgreSQL is the authoritative store for those.
    """
    schema_map = {p.position_number: p for p in schema_positions}

    with driver.session() as neo_session:
        sorted_pos = sorted(resolved_positions, key=lambda x: x[0])
        for i in range(len(sorted_pos) - 1):
            from_pos_num, from_item_id = sorted_pos[i]
            to_pos_num, to_item_id = sorted_pos[i + 1]
            from_schema = schema_map[from_pos_num]
            to_schema = schema_map[to_pos_num]

            from_type = getattr(from_schema, 'node_type', 'CORE')
            to_type = getattr(to_schema, 'node_type', 'CORE')
            if from_type == 'CONNECT' or to_type == 'CONNECT':
                continue

            rel_type = from_schema.relationship_type_to_next

            neo_session.run(
                f"""
                MATCH (a:CoreDataItem {{id: $from_id}})
                MATCH (b:CoreDataItem {{id: $to_id}})
                CREATE (a)-[:{rel_type} {{
                    connect_data_item_id: $cdi_id,
                    connect_id: $connect_id,
                    schema_position_from: $from_pos,
                    schema_position_to: $to_pos,
                    status: 'ACTIVE'
                }}]->(b)
                """,
                from_id=from_item_id,
                to_id=to_item_id,
                cdi_id=connect_data_item_id,
                connect_id=connect_id,
                from_pos=from_pos_num,
                to_pos=to_pos_num,
            )


def inactivate_neo4j_relationships(connect_data_item_id: str):
    """BL-C-05 (connect): set all Neo4J relationships for this CDI to INACTIVE."""
    with driver.session() as neo_session:
        neo_session.run(
            "MATCH ()-[r {connect_data_item_id: $id}]-() SET r.status = 'INACTIVE'",
            id=connect_data_item_id
        )
