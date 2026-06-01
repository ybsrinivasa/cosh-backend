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
    `group` is a UI hint set to "filter1" or "filter2" so the renderer can
    style the two slice sides differently (e.g., larger / glowing nodes
    for the focal slice).
    """
    id: str
    label: str
    core_id: str
    core_name: str
    group: Literal["filter1", "filter2"]


class VizEdge(BaseModel):
    source: str  # node id
    target: str  # node id
    rel_type: str
    connect_id: str
    # Optional human-readable label sourced from the Connect's name — handy
    # for tooltips. We resolve it server-side so the renderer stays dumb.
    connect_name: Optional[str] = None


class SliceOut(BaseModel):
    nodes: list[VizNode]
    edges: list[VizEdge]
    truncated: bool  # True if hit the result cap — UI can warn the user
