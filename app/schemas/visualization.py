"""Visualization Module schemas.

The slice endpoint returns a {nodes, edges} payload that the frontend
hands straight to react-force-graph-3d. Keep the shape minimal — every
extra field per node/edge is paid for at render time on 200–500 elements.
"""
from typing import Optional, Literal
from pydantic import BaseModel


# ── Filter-options endpoint ──────────────────────────────────────────────────

class CoreOption(BaseModel):
    """One entry in the filter dropdown — a whole Core ("All Crops").

    `active_item_count` lets the UI hide Cores with nothing in them so the
    dropdown doesn't list dead categories.
    """
    id: str
    name: str
    active_item_count: int


class ItemOption(BaseModel):
    """One item within a Core ("Tomato"). Used by the autocomplete inside
    a chosen Core in the cascading filter."""
    id: str
    english_value: str
    core_id: str


class FilterOptionsOut(BaseModel):
    cores: list[CoreOption]


# ── Connect filter options (Phase 3.2) ───────────────────────────────────────

class ConnectOption(BaseModel):
    """One entry in the Connect-mode dropdown. `position_count` lets the UI
    flag multi-position Connects (which are the most interesting to show)."""
    id: str
    name: str
    active_item_count: int
    position_count: int


class ConnectListOut(BaseModel):
    connects: list[ConnectOption]


# ── Search endpoint ──────────────────────────────────────────────────────────

class SearchHit(BaseModel):
    id: str
    english_value: str
    core_id: str
    core_name: str


class SearchOut(BaseModel):
    hits: list[SearchHit]


# ── Slice endpoint (the main viz payload) ────────────────────────────────────

class VizNode(BaseModel):
    """A node in the rendered subgraph.

    `core_id` drives stable colouring on the frontend (one colour per Core).
    `group` is a UI hint — "filter1" for the focal/primary side, "filter2"
    for the connected side. When Filter 2 is omitted, every neighbour is
    tagged "filter2" so the renderer keeps the same visual differentiation.

    `node_kind` lets the renderer distinguish ordinary Core items from
    virtual "hub" nodes produced by /viz/connect-slice (one hub per
    ConnectDataItem). Hubs use the Connect name as `core_name` so they
    get their own palette slot.
    """
    id: str
    label: str
    core_id: str
    core_name: str
    group: Literal["filter1", "filter2"]
    node_kind: Literal["item", "hub"] = "item"


class VizEdge(BaseModel):
    source: str  # node id
    target: str  # node id
    rel_type: str
    connect_id: str
    # Optional human-readable label sourced from the Connect's name — handy
    # for tooltips. We resolve it server-side so the renderer stays dumb.
    connect_name: Optional[str] = None
    # How many rows of the source Connect produced this edge. Used by the
    # renderer to scale line width — thicker = stronger relationship.
    weight: int = 1
    # IDs of the rows that produced this edge. Empty / omitted for edges
    # that don't originate from a Connect's rows (e.g., slice mode). When
    # populated, the UI can show all participating rows on edge click.
    row_ids: Optional[list[str]] = None


class SliceOut(BaseModel):
    nodes: list[VizNode]
    edges: list[VizEdge]
    truncated: bool  # True if hit the result cap — UI can warn the user
