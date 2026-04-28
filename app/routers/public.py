"""
P8-01: Public Knowledge Visualisation API — no authentication required.
Returns graph data (nodes + edges) from the public subset of the Cosh knowledge graph.
Designed for consumption by react-force-graph or Sigma.js on eywa.farm/knowledge.
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.database import get_db
from app.models.models import Core, Connect, ConnectSchemaPosition, Folder, StatusEnum

router = APIRouter(prefix="/public", tags=["Public Knowledge"])


@router.get("/knowledge/filters")
async def get_knowledge_filters(db: AsyncSession = Depends(get_db)):
    """
    Returns the cascading filter options for the knowledge graph UI.
    Structure: folders (domains) → public cores within each folder (categories).
    """
    public_cores = (await db.execute(
        select(Core)
        .where(Core.is_public == True, Core.status == StatusEnum.ACTIVE)
        .order_by(Core.name)
    )).scalars().all()

    if not public_cores:
        return {"domains": []}

    folder_ids = list({c.folder_id for c in public_cores})
    folders = (await db.execute(
        select(Folder).where(Folder.id.in_(folder_ids)).order_by(Folder.name)
    )).scalars().all()
    folder_map = {f.id: f.name for f in folders}

    domains = {}
    for core in public_cores:
        folder_name = folder_map.get(core.folder_id, "Uncategorised")
        if folder_name not in domains:
            domains[folder_name] = {
                "folder_id": core.folder_id,
                "name": folder_name,
                "cores": [],
            }
        domains[folder_name]["cores"].append({
            "core_id": core.id,
            "name": core.name,
            "description": core.description,
        })

    public_connects = (await db.execute(
        select(Connect.id, Connect.name, Connect.description)
        .where(Connect.is_public == True, Connect.status == StatusEnum.ACTIVE)
        .order_by(Connect.name)
    )).all()

    return {
        "domains": list(domains.values()),
        "relationship_types": [
            {"connect_id": r.id, "name": r.name, "description": r.description}
            for r in public_connects
        ],
    }


@router.get("/knowledge")
async def get_knowledge_graph(
    db: AsyncSession = Depends(get_db),
    folder_id: str = Query(None, description="Filter to cores in this folder"),
    core_id: str = Query(None, description="Filter to nodes from this core only"),
):
    """
    P8-01: Returns the public knowledge graph as nodes + links for 3D graph rendering.

    Nodes: active Core Data Items from public Cores.
    Links: active relationships from public Connects where both endpoints are in public Cores.

    Optional filters:
    - folder_id: show only cores belonging to this folder (domain)
    - core_id: show only items from this core (and their direct connections)
    """
    from app.neo4j_db import driver

    # Get public core IDs, optionally filtered by folder
    core_q = select(Core).where(Core.is_public == True, Core.status == StatusEnum.ACTIVE)
    if folder_id:
        core_q = core_q.where(Core.folder_id == folder_id)
    if core_id:
        core_q = core_q.where(Core.id == core_id)

    public_cores = (await db.execute(core_q)).scalars().all()
    if not public_cores:
        return {"nodes": [], "links": [], "meta": {"total_nodes": 0, "total_links": 0}}

    public_core_ids = [c.id for c in public_cores]
    core_name_map = {c.id: c.name for c in public_cores}

    # Get public connect IDs (only those whose ALL positions are in public cores)
    public_connect_ids = []
    public_connects = (await db.execute(
        select(Connect).where(Connect.is_public == True, Connect.status == StatusEnum.ACTIVE)
    )).scalars().all()

    for connect in public_connects:
        positions = (await db.execute(
            select(ConnectSchemaPosition.core_id)
            .where(ConnectSchemaPosition.connect_id == connect.id)
        )).scalars().all()
        # Only include the connect if all its position cores are in the public set
        if all(cid in public_core_ids for cid in positions):
            public_connect_ids.append(connect.id)

    connect_name_map = {c.id: c.name for c in public_connects if c.id in public_connect_ids}

    # Query Neo4J for nodes and relationships
    nodes = []
    links = []

    with driver.session() as neo_session:
        # Fetch all active nodes from public cores
        node_result = neo_session.run(
            """
            MATCH (n:CoreDataItem {status: 'ACTIVE'})
            WHERE n.core_id IN $core_ids
            RETURN n.id AS id, n.english_value AS label, n.core_id AS core_id
            """,
            core_ids=public_core_ids,
        )
        node_ids_in_graph = set()
        for record in node_result:
            nodes.append({
                "id": record["id"],
                "label": record["label"],
                "core_id": record["core_id"],
                "group": core_name_map.get(record["core_id"], ""),
            })
            node_ids_in_graph.add(record["id"])

        # Fetch active relationships from public connects
        if public_connect_ids and node_ids_in_graph:
            rel_result = neo_session.run(
                """
                MATCH (a:CoreDataItem {status: 'ACTIVE'})-[r {status: 'ACTIVE'}]->(b:CoreDataItem {status: 'ACTIVE'})
                WHERE r.connect_id IN $connect_ids
                  AND a.core_id IN $core_ids
                  AND b.core_id IN $core_ids
                RETURN a.id AS source, b.id AS target,
                       type(r) AS rel_type, r.connect_id AS connect_id,
                       r.connect_data_item_id AS connect_data_item_id
                """,
                connect_ids=public_connect_ids,
                core_ids=public_core_ids,
            )
            seen_links = set()
            for record in rel_result:
                key = (record["source"], record["target"], record["rel_type"])
                if key in seen_links:
                    continue
                seen_links.add(key)
                links.append({
                    "source": record["source"],
                    "target": record["target"],
                    "type": record["rel_type"],
                    "connect_id": record["connect_id"],
                    "connect_name": connect_name_map.get(record["connect_id"], ""),
                })

    return {
        "nodes": nodes,
        "links": links,
        "meta": {
            "total_nodes": len(nodes),
            "total_links": len(links),
            "public_cores": [
                {"id": c.id, "name": c.name, "folder_id": c.folder_id}
                for c in public_cores
            ],
            "public_connects": [
                {"id": cid, "name": connect_name_map[cid]}
                for cid in public_connect_ids
            ],
        },
    }
