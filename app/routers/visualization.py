"""Visualization Module — backend.

Three endpoints feed the cascading filter + 3D graph canvas:

  GET /viz/filter-options             list Cores for the primary dropdown
  GET /viz/filter-options?connected_to_core={id}
  GET /viz/filter-options?connected_to_item={id}
                                       narrowed list for the secondary
                                       dropdown — only Cores with at least
                                       one ACTIVE relationship to the primary

  GET /viz/search?q=tomato            fulltext-ish search inside a Core or
                                       across all Cores; powers in-dropdown
                                       autocomplete for picking a specific
                                       item rather than a whole Core

  GET /viz/slice?...                  the actual subgraph: nodes + edges
                                       matching the two filters, suitable
                                       to hand straight to the renderer

Every Neo4J query hard-filters `r.status='ACTIVE'` and `n.status='ACTIVE'`
per the visualization-active-only rule.
"""
from typing import Optional, Literal
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.dependencies import require_role
from app.models.models import (
    Core, CoreDataItem, Connect, StatusEnum, UserRole,
)
from app.neo4j_db import driver
from app.schemas.visualization import (
    FilterOptionsOut, CoreOption,
    SearchOut, SearchHit,
    SliceOut, VizNode, VizEdge,
)

router = APIRouter(prefix="/viz", tags=["visualization"])

require_viz_user = require_role(UserRole.ADMIN, UserRole.DESIGNER)

# Hard cap on returned edges. 500 keeps the 3D canvas readable; we surface
# `truncated: True` so the UI can prompt the user to tighten filters.
MAX_EDGES = 500

# Over-fetch raw Neo4J records so the Python dedup pass has a fair chance
# to surface MAX_EDGES of unique edges even when the data has heavy
# duplication (the State->District chain shows ~33x raw rows per unique
# edge locally). Without this, a slice could cap at 500 raw matches that
# collapse to ~15 visible edges, hiding the rest.
RAW_LIMIT = MAX_EDGES * 10


# ── /viz/filter-options ──────────────────────────────────────────────────────

@router.get("/filter-options", response_model=FilterOptionsOut)
async def filter_options(
    connected_to_core: Optional[str] = Query(None, description="Cascade: only return Cores with ACTIVE edges to a node in this Core"),
    connected_to_item: Optional[str] = Query(None, description="Cascade: only return Cores with ACTIVE edges to this specific CoreDataItem"),
    db: AsyncSession = Depends(get_db),
    _user=Depends(require_viz_user),
):
    """Return Cores eligible for the visualization filter dropdown.

    No cascade: all Cores that have at least one ACTIVE item.
    With cascade: only Cores that have at least one ACTIVE relationship
    landing in a node from the primary selection.
    """
    if connected_to_core and connected_to_item:
        raise HTTPException(
            status_code=422,
            detail="Pass either connected_to_core or connected_to_item, not both",
        )

    # Postgres: every Core with its active item count. This is the ground
    # truth for what's clickable — Neo4J is only consulted for cascading.
    rows = (await db.execute(
        select(Core.id, Core.name, func.count(CoreDataItem.id))
        .join(CoreDataItem, CoreDataItem.core_id == Core.id, isouter=True)
        .where(CoreDataItem.status == StatusEnum.ACTIVE)
        .where(Core.status == StatusEnum.ACTIVE)
        .group_by(Core.id, Core.name)
        .order_by(Core.name)
    )).all()
    cores_by_id = {r[0]: CoreOption(id=r[0], name=r[1], active_item_count=r[2]) for r in rows}

    if not connected_to_core and not connected_to_item:
        return FilterOptionsOut(cores=list(cores_by_id.values()))

    # Cascading: ask Neo4J which Cores have any ACTIVE rel landing in the
    # selected primary, then filter the Postgres list down.
    with driver.session() as s:
        if connected_to_core:
            result = s.run(
                """
                MATCH (a:CoreDataItem)-[r]-(b:CoreDataItem)
                WHERE a.core_id = $cid
                  AND a.status = 'ACTIVE'
                  AND b.status = 'ACTIVE'
                  AND r.status = 'ACTIVE'
                  AND b.core_id <> $cid
                RETURN DISTINCT b.core_id AS core_id
                """,
                cid=connected_to_core,
            )
        else:
            result = s.run(
                """
                MATCH (a:CoreDataItem {id: $iid})-[r]-(b:CoreDataItem)
                WHERE a.status = 'ACTIVE'
                  AND b.status = 'ACTIVE'
                  AND r.status = 'ACTIVE'
                RETURN DISTINCT b.core_id AS core_id
                """,
                iid=connected_to_item,
            )
        reachable_core_ids = {r["core_id"] for r in result}

    filtered = [cores_by_id[cid] for cid in reachable_core_ids if cid in cores_by_id]
    filtered.sort(key=lambda c: c.name)
    return FilterOptionsOut(cores=filtered)


# ── /viz/search ──────────────────────────────────────────────────────────────

@router.get("/search", response_model=SearchOut)
async def search_items(
    q: str = Query(..., min_length=1, max_length=200),
    core_id: Optional[str] = Query(None, description="Narrow the search to one Core"),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    _user=Depends(require_viz_user),
):
    """Autocomplete-style item search.

    Used by the cascading filter when the user wants to pick a SPECIFIC
    item (e.g., "Tomato") instead of a whole Core ("All Crops").
    Postgres ILIKE is plenty fast at our scale (<20k items) and avoids
    a second Neo4J round trip.
    """
    stmt = (
        select(CoreDataItem.id, CoreDataItem.english_value, CoreDataItem.core_id, Core.name)
        .join(Core, Core.id == CoreDataItem.core_id)
        .where(CoreDataItem.status == StatusEnum.ACTIVE)
        .where(CoreDataItem.english_value.ilike(f"%{q}%"))
        .order_by(
            # Exact-prefix matches first, then any contains.
            CoreDataItem.english_value.ilike(f"{q}%").desc(),
            func.length(CoreDataItem.english_value),
            CoreDataItem.english_value,
        )
        .limit(limit)
    )
    if core_id:
        stmt = stmt.where(CoreDataItem.core_id == core_id)
    rows = (await db.execute(stmt)).all()
    return SearchOut(hits=[
        SearchHit(id=r[0], english_value=r[1], core_id=r[2], core_name=r[3])
        for r in rows
    ])


# ── /viz/slice ───────────────────────────────────────────────────────────────

FilterType = Literal["core", "item"]


@router.get("/slice", response_model=SliceOut)
async def slice_(
    filter1_type: FilterType = Query(..., description="'core' = whole Core, 'item' = single CoreDataItem"),
    filter1_id: str = Query(..., min_length=36, max_length=36),
    filter2_type: FilterType = Query(...),
    filter2_id: str = Query(..., min_length=36, max_length=36),
    db: AsyncSession = Depends(get_db),
    _user=Depends(require_viz_user),
):
    """Return the subgraph (nodes + edges) implied by the two filters.

    Semantics:
      CORE  -> CORE : bipartite between all items in the two Cores.
      CORE  -> ITEM : items in Core 1 that touch the chosen item.
      ITEM  -> CORE : the chosen item + its neighbours within Core 2.
      ITEM  -> ITEM : just the direct edges between the two items (often
                       a single edge or empty — useful for inspecting a
                       known pair).

    `group` on each node tells the renderer which side of the slice the
    node belongs to so the frontend can size or colour-emphasise it.
    """
    a_filter = "a.core_id = $f1_id" if filter1_type == "core" else "a.id = $f1_id"
    b_filter = "b.core_id = $f2_id" if filter2_type == "core" else "b.id = $f2_id"

    cypher = f"""
    MATCH (a:CoreDataItem)-[r]-(b:CoreDataItem)
    WHERE {a_filter}
      AND {b_filter}
      AND a.status = 'ACTIVE'
      AND b.status = 'ACTIVE'
      AND r.status = 'ACTIVE'
      AND a.id <> b.id
    RETURN a.id AS a_id, a.core_id AS a_core, a.english_value AS a_label,
           b.id AS b_id, b.core_id AS b_core, b.english_value AS b_label,
           type(r) AS rel_type, r.connect_id AS connect_id
    LIMIT $cap
    """

    with driver.session() as s:
        records = list(s.run(
            cypher,
            f1_id=filter1_id,
            f2_id=filter2_id,
            cap=RAW_LIMIT,
        ))
    raw_hit_cap = len(records) >= RAW_LIMIT

    # Look up display names for every Core and Connect touched, in one
    # Postgres round-trip each. The renderer expects display names rather
    # than UUIDs in tooltips.
    core_ids = {rec["a_core"] for rec in records} | {rec["b_core"] for rec in records}
    connect_ids = {rec["connect_id"] for rec in records if rec["connect_id"]}

    core_names: dict[str, str] = {}
    if core_ids:
        rows = (await db.execute(
            select(Core.id, Core.name).where(Core.id.in_(core_ids))
        )).all()
        core_names = {r[0]: r[1] for r in rows}

    connect_names: dict[str, str] = {}
    if connect_ids:
        rows = (await db.execute(
            select(Connect.id, Connect.name).where(Connect.id.in_(connect_ids))
        )).all()
        connect_names = {r[0]: r[1] for r in rows}

    # Pick which side of the slice each unique node belongs to. A node can
    # in principle match both filters (e.g., when filter1 == filter2 by
    # mistake); we err on the side of "filter1" to keep the focal side
    # visually stable.
    def _matches_filter1(core_id: str, item_id: str) -> bool:
        return (filter1_type == "core" and core_id == filter1_id) or \
               (filter1_type == "item" and item_id == filter1_id)

    nodes: dict[str, VizNode] = {}
    for rec in records:
        for nid, ncore, nlabel in (
            (rec["a_id"], rec["a_core"], rec["a_label"]),
            (rec["b_id"], rec["b_core"], rec["b_label"]),
        ):
            if nid in nodes:
                continue
            group: Literal["filter1", "filter2"] = (
                "filter1" if _matches_filter1(ncore, nid) else "filter2"
            )
            nodes[nid] = VizNode(
                id=nid,
                label=nlabel or "",
                core_id=ncore,
                core_name=core_names.get(ncore, ""),
                group=group,
            )

    # Edges: one per (rel_type, connect_id, source, target). A given pair
    # of nodes can have multiple edges (same nodes, different Connects)
    # and we keep them so the canvas shows all the relationships.
    edges: list[VizEdge] = []
    seen_edge_keys: set[tuple] = set()
    for rec in records:
        key = (rec["rel_type"], rec["connect_id"], rec["a_id"], rec["b_id"])
        # Treat (a,b) and (b,a) on the SAME connect as one logical edge —
        # the underlying rel is directed in storage but the canvas draws
        # it once.
        canon = tuple(sorted([rec["a_id"], rec["b_id"]]))
        canon_key = (rec["rel_type"], rec["connect_id"], canon[0], canon[1])
        if canon_key in seen_edge_keys:
            continue
        seen_edge_keys.add(canon_key)
        edges.append(VizEdge(
            source=rec["a_id"],
            target=rec["b_id"],
            rel_type=rec["rel_type"],
            connect_id=rec["connect_id"] or "",
            connect_name=connect_names.get(rec["connect_id"]),
        ))

    # Apply the final edge cap after dedup. Drop any nodes that only appear
    # on dropped edges so we don't render orphan dots in the canvas.
    if len(edges) > MAX_EDGES:
        edges = edges[:MAX_EDGES]
        kept_node_ids: set[str] = set()
        for e in edges:
            kept_node_ids.add(e.source)
            kept_node_ids.add(e.target)
        nodes = {nid: n for nid, n in nodes.items() if nid in kept_node_ids}
        truncated = True
    else:
        # Truncated is also True if Cypher itself capped — there may be
        # more matches we never even examined.
        truncated = raw_hit_cap

    return SliceOut(
        nodes=list(nodes.values()),
        edges=edges,
        truncated=truncated,
    )
