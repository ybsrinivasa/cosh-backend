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
from sqlalchemy.orm import selectinload
from app.models.models import (
    Core, CoreDataItem, Connect, ConnectSchemaPosition, ConnectDataItem,
    NodeType, StatusEnum, UserRole,
)
from app.neo4j_db import driver
from app.schemas.visualization import (
    FilterOptionsOut, CoreOption,
    ConnectListOut, ConnectOption,
    SearchOut, SearchHit,
    SliceOut, VizNode, VizEdge,
)

router = APIRouter(prefix="/viz", tags=["visualization"])

require_viz_user = require_role(UserRole.ADMIN, UserRole.DESIGNER)

# Hard cap on returned edges. 200 keeps the 3D canvas readable AND keeps
# the per-frame physics + label overlay work under a budget that holds up
# during long demo sessions (was 500; lowered 2026-06-15 after a demo to
# Karnataka Agri Dept exposed sustained-load hangs on bigger slices).
# We surface `truncated: True` so the UI can prompt the user to tighten
# filters when the cap kicks in.
MAX_EDGES = 200

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
    filter2_type: Optional[FilterType] = Query(None, description="Omit to render everything connected to filter 1"),
    filter2_id: Optional[str] = Query(None, min_length=36, max_length=36),
    db: AsyncSession = Depends(get_db),
    _user=Depends(require_viz_user),
):
    """Return the subgraph (nodes + edges) implied by the filter(s).

    Semantics:
      filter1 only       : everything 1 hop from filter 1 (whole Core or
                            single item), regardless of which Core the
                            neighbour lives in.
      CORE  -> CORE      : bipartite between all items in the two Cores.
      CORE  -> ITEM      : items in Core 1 that touch the chosen item.
      ITEM  -> CORE      : the chosen item + its neighbours within Core 2.
      ITEM  -> ITEM      : direct edges between the two items.

    `group` on each node tells the renderer which side of the slice the
    node belongs to so the frontend can size or colour-emphasise it.
    """
    if (filter2_type is None) != (filter2_id is None):
        raise HTTPException(422, "filter2_type and filter2_id must both be set or both be omitted")

    a_filter = "a.core_id = $f1_id" if filter1_type == "core" else "a.id = $f1_id"
    if filter2_type is None:
        # "Show everything connected to filter1" — no constraint on b's identity.
        b_filter = "true"
    else:
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

    params = {"f1_id": filter1_id, "cap": RAW_LIMIT}
    if filter2_id is not None:
        params["f2_id"] = filter2_id

    with driver.session() as s:
        records = list(s.run(cypher, **params))
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
    # visually stable. When filter2 is omitted, anything that isn't filter1
    # is treated as a connected neighbour ("filter2") so the renderer's
    # styling rule stays the same regardless of mode.
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


# ── /viz/connects-list (Phase 3.2) ───────────────────────────────────────────

@router.get("/connects-list", response_model=ConnectListOut)
async def connects_list(
    db: AsyncSession = Depends(get_db),
    _user=Depends(require_viz_user),
):
    """Connects with active items, suitable for the "Connect mode"
    dropdown. We surface position_count so the UI can flag the
    multi-position ones (those are the visually richest in connect-slice
    mode — each row becomes a hub with multiple spokes)."""
    cdi_counts_subq = (
        select(ConnectDataItem.connect_id, func.count(ConnectDataItem.id).label("c"))
        .where(ConnectDataItem.status == StatusEnum.ACTIVE)
        .group_by(ConnectDataItem.connect_id)
        .subquery()
    )
    pos_counts_subq = (
        select(ConnectSchemaPosition.connect_id, func.count(ConnectSchemaPosition.id).label("c"))
        .group_by(ConnectSchemaPosition.connect_id)
        .subquery()
    )
    rows = (await db.execute(
        select(Connect.id, Connect.name, cdi_counts_subq.c.c, pos_counts_subq.c.c)
        .join(cdi_counts_subq, cdi_counts_subq.c.connect_id == Connect.id)
        .join(pos_counts_subq, pos_counts_subq.c.connect_id == Connect.id, isouter=True)
        .where(Connect.status == StatusEnum.ACTIVE)
        .order_by(Connect.name)
    )).all()
    return ConnectListOut(connects=[
        ConnectOption(
            id=r[0], name=r[1],
            active_item_count=r[2] or 0,
            position_count=r[3] or 0,
        )
        for r in rows
    ])


# ── /viz/connect-slice (Phase 3.2 / Full Mode 2026-06-15) ────────────────────
# Full Mode rendering (2026-06-15 redesign):
#   No hub nodes. Each row is "flattened" into pairwise edges between every
#   pair of CoreDataItems in that row. So a Pest Diagnosis row touching
#   {Tomato, Vegetative, Aphid, Larva, Leaf, Yellow Spots} contributes 15
#   edges (C(6,2)). When Tomato and Leaf co-occur in 50 rows, you see 50
#   parallel edges — the frontend fans them out via curvature+rotation so
#   they look bundled when zoomed out and individual when zoomed in.
#
#   Each edge carries `row_id` (the source ConnectDataItem.id) so the UI
#   can later show "this specific strand was Aphid/Larva/Yellow Spots"
#   on hover or click.
#
#   This replaces the old hub-and-spoke model where every row became a hub
#   with N spokes to its items. The hub renderer felt "chaotic" in demos
#   because 100 rows = 100 visually similar hubs around a small set of
#   reused leaves.

# Row cap. We don't dedupe edges (the whole point is to show every strand),
# so the edge count grows like ROW_LIMIT × C(items_per_row, 2). For a
# 6-position Connect that's 15× — so 200 rows → ~3,000 edges max.
ROW_LIMIT = 200


@router.get("/connect-slice", response_model=SliceOut)
async def connect_slice(
    connect_id: str = Query(..., min_length=36, max_length=36),
    anchor_id: Optional[str] = Query(
        None, min_length=36, max_length=36,
        description="If set, only include rows that touch this CoreDataItem (either "
                    "directly via a CORE position or transitively through a CONNECT "
                    "position's referenced row).",
    ),
    db: AsyncSession = Depends(get_db),
    _user=Depends(require_viz_user),
):
    connect = (await db.execute(
        select(Connect).where(Connect.id == connect_id)
    )).scalar_one_or_none()
    if not connect:
        raise HTTPException(404, "Connect not found")

    # Schema for the target Connect tells us which positions are CORE vs
    # CONNECT-references. We need this to know which positions to follow.
    schema = (await db.execute(
        select(ConnectSchemaPosition)
        .where(ConnectSchemaPosition.connect_id == connect_id)
        .order_by(ConnectSchemaPosition.position_number)
    )).scalars().all()
    if not schema:
        raise HTTPException(422, "Connect has no schema positions")

    # Active rows of the target Connect. selectinload pulls each row's
    # positions in one extra query.
    rows = (await db.execute(
        select(ConnectDataItem)
        .options(selectinload(ConnectDataItem.positions))
        .where(
            ConnectDataItem.connect_id == connect_id,
            ConnectDataItem.status == StatusEnum.ACTIVE,
        )
        .order_by(ConnectDataItem.created_at)
    )).scalars().all()

    # For every CONNECT-type position that any row uses, pre-fetch the
    # referenced rows + their positions so we can resolve the spoke set
    # for each hub without N round-trips.
    referenced_cdi_ids: set[str] = set()
    for row in rows:
        for pos in row.positions:
            if pos.connect_data_item_ref_id:
                referenced_cdi_ids.add(pos.connect_data_item_ref_id)

    ref_row_items: dict[str, list[str]] = {}
    if referenced_cdi_ids:
        ref_rows = (await db.execute(
            select(ConnectDataItem)
            .options(selectinload(ConnectDataItem.positions))
            .where(ConnectDataItem.id.in_(referenced_cdi_ids))
        )).scalars().all()
        for ref in ref_rows:
            ref_row_items[ref.id] = [
                p.core_data_item_id for p in ref.positions if p.core_data_item_id
            ]

    # Build the (hub_id, [resolved_core_item_ids]) list, applying the
    # anchor filter if given.
    resolved_rows: list[tuple[str, list[str]]] = []
    for row in rows:
        items: list[str] = []
        for pos in row.positions:
            if pos.core_data_item_id:
                items.append(pos.core_data_item_id)
            elif pos.connect_data_item_ref_id:
                items.extend(ref_row_items.get(pos.connect_data_item_ref_id, []))
        if anchor_id and anchor_id not in items:
            continue
        resolved_rows.append((row.id, items))

    truncated = len(resolved_rows) > ROW_LIMIT
    resolved_rows = resolved_rows[:ROW_LIMIT]

    # Lookup labels + Core metadata for every touched item.
    all_item_ids = {iid for _, items in resolved_rows for iid in items}
    items_by_id: dict[str, dict] = {}
    if all_item_ids:
        item_rows = (await db.execute(
            select(CoreDataItem.id, CoreDataItem.english_value, CoreDataItem.core_id)
            .where(
                CoreDataItem.id.in_(all_item_ids),
                CoreDataItem.status == StatusEnum.ACTIVE,
            )
        )).all()
        items_by_id = {
            r[0]: {"english_value": r[1], "core_id": r[2]} for r in item_rows
        }

    core_name_by_id: dict[str, str] = {}
    touched_core_ids = {x["core_id"] for x in items_by_id.values()}
    if touched_core_ids:
        c_rows = (await db.execute(
            select(Core.id, Core.name).where(Core.id.in_(touched_core_ids))
        )).all()
        core_name_by_id = {r[0]: r[1] for r in c_rows}

    nodes: dict[str, VizNode] = {}
    edges: list[VizEdge] = []

    for row_id, item_ids in resolved_rows:
        # Filter to items we actually have metadata for, deduping within a
        # row (a row never repeats positions but a CONNECT-typed position
        # could resolve to a referenced row that shares an item — safer to
        # dedupe).
        valid_item_ids = []
        seen: set[str] = set()
        for iid in item_ids:
            if iid in seen:
                continue
            if iid not in items_by_id:
                continue
            seen.add(iid)
            valid_item_ids.append(iid)

        # Add each touched item as a canonical node (once across the slice).
        for iid in valid_item_ids:
            if iid in nodes:
                continue
            md = items_by_id[iid]
            nodes[iid] = VizNode(
                id=iid,
                label=md["english_value"] or "",
                core_id=md["core_id"],
                core_name=core_name_by_id.get(md["core_id"], ""),
                group="filter2",
                node_kind="item",
            )

        # Emit one edge per pair of items in this row — C(N,2) edges.
        # Sort the pair so a→b and b→a collapse to the same canonical
        # direction (still one edge per row instance, but consistent
        # source/target lets the frontend group parallel strands cleanly).
        for i in range(len(valid_item_ids)):
            for j in range(i + 1, len(valid_item_ids)):
                a, b = valid_item_ids[i], valid_item_ids[j]
                if a > b:
                    a, b = b, a
                edges.append(VizEdge(
                    source=a,
                    target=b,
                    rel_type="IN_ROW",
                    connect_id=connect.id,
                    connect_name=connect.name,
                    row_id=row_id,
                ))

    return SliceOut(
        nodes=list(nodes.values()),
        edges=edges,
        truncated=truncated,
    )
