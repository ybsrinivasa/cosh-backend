"""
Reconcile Neo4J with Postgres for Cosh 2.0.

What it does (idempotent):
  Nodes (CoreDataItem):
    1. Insert any PG row missing in Neo4J.
    2. Update status property where it disagrees with PG.
    3. Delete any Neo4J node whose id has no PG row (orphan).
  Relationships:
    1. Insert any ConnectDataPosition-pair missing in Neo4J.
    2. Update status property where it disagrees with the parent Connect.
    3. Delete any Neo4J relationship whose connect_data_item_id has no
       active PG ConnectDataItem (orphan).
  Indexes:
    1. Add relationship indexes on .status and .connect_id if missing
       (visualization queries always filter on these).

Run:
  python scripts/resync_neo4j.py            # apply
  python scripts/resync_neo4j.py --dry-run  # report only

Designed to be safe on prod: every write is batched via UNWIND and
status is read directly from Postgres (source of truth).
"""
import argparse
import asyncio
import os
import sys
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select
from sqlalchemy.orm import selectinload
from app.database import AsyncSessionLocal
from app.models.models import (
    CoreDataItem, ConnectDataItem, ConnectDataPosition,
    ConnectSchemaPosition, StatusEnum,
)
from app.neo4j_db import driver


NEO4J_BATCH = 1000

# Neo4J 5 requires a specific relationship type for each index. The viz layer
# will mostly filter on r.status and r.connect_id, so we add both per type.
REL_INDEX_PROPS = ["status", "connect_id"]


async def load_postgres_state():
    """Snapshot what Postgres thinks the graph should look like.

    Returns:
      cdi_status        dict[cdi_id -> 'ACTIVE'|'INACTIVE']
      cdi_meta          dict[cdi_id -> dict(core_id, english_value)]
      rel_specs         list[dict] — every expected relationship, one per
                        adjacent ConnectDataPosition pair within an ACTIVE
                        ConnectDataItem.
      active_cdi_ids    set[ConnectDataItem.id] (active only)
    """
    async with AsyncSessionLocal() as db:
        cdi_rows = (await db.execute(
            select(CoreDataItem.id, CoreDataItem.core_id,
                   CoreDataItem.english_value, CoreDataItem.status)
        )).all()
        cdi_status = {r.id: r.status.value for r in cdi_rows}
        cdi_meta = {
            r.id: {"core_id": r.core_id, "english_value": r.english_value}
            for r in cdi_rows
        }

        sp_by_connect = defaultdict(list)
        for sp in (await db.execute(select(ConnectSchemaPosition))).scalars():
            sp_by_connect[sp.connect_id].append(sp)
        for sps in sp_by_connect.values():
            sps.sort(key=lambda s: s.position_number)

        connect_items = (await db.execute(
            select(ConnectDataItem)
            .options(selectinload(ConnectDataItem.positions))
            .where(ConnectDataItem.status == StatusEnum.ACTIVE)
        )).scalars().all()
        active_cdi_ids = {i.id for i in connect_items}

        rel_specs = []
        for ci in connect_items:
            schema = sp_by_connect.get(ci.connect_id, [])
            schema_by_pos = {s.position_number: s for s in schema}
            positions = sorted(ci.positions, key=lambda p: p.position_number)
            # Build a relationship between each adjacent CORE-CORE position
            # pair. CONNECT-typed positions don't get Neo4J rels — they
            # reference a row in another Connect.
            for j in range(len(positions) - 1):
                a, b = positions[j], positions[j + 1]
                sa = schema_by_pos.get(a.position_number)
                sb = schema_by_pos.get(b.position_number)
                if not sa or not sb:
                    continue
                ta = sa.node_type.value if hasattr(sa.node_type, "value") else str(sa.node_type)
                tb = sb.node_type.value if hasattr(sb.node_type, "value") else str(sb.node_type)
                if ta != "CORE" or tb != "CORE":
                    continue
                if not a.core_data_item_id or not b.core_data_item_id:
                    continue
                rel_type = sa.relationship_type_to_next
                if not rel_type:
                    continue
                rel_specs.append({
                    "from_id": a.core_data_item_id,
                    "to_id": b.core_data_item_id,
                    "cdi_id": ci.id,
                    "connect_id": ci.connect_id,
                    "from_pos": a.position_number,
                    "to_pos": b.position_number,
                    "rel_type": rel_type,
                })

    return cdi_status, cdi_meta, rel_specs, active_cdi_ids


def load_neo4j_state():
    """Snapshot what Neo4J currently has."""
    with driver.session() as s:
        nodes = list(s.run(
            "MATCH (n:CoreDataItem) RETURN n.id AS id, n.status AS status"
        ))
        rels = list(s.run(
            "MATCH (a:CoreDataItem)-[r]->(b:CoreDataItem) "
            "RETURN type(r) AS rel_type, a.id AS from_id, b.id AS to_id, "
            "r.connect_data_item_id AS cdi_id, r.status AS status"
        ))
        rel_indexes = list(s.run("SHOW INDEXES YIELD name"))
    return nodes, rels, {r["name"] for r in rel_indexes}


def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Report deltas without writing anything")
    args = parser.parse_args()

    print("Loading Postgres state ...")
    cdi_status, cdi_meta, rel_specs, active_cdi_ids = asyncio.run(load_postgres_state())
    print(f"  CoreDataItem rows: {len(cdi_status)}  (active={sum(1 for v in cdi_status.values() if v == 'ACTIVE')})")
    print(f"  Active ConnectDataItem rows: {len(active_cdi_ids)}")
    print(f"  Expected CORE-CORE relationships: {len(rel_specs)}")

    print("Loading Neo4J state ...")
    neo_nodes, neo_rels, neo_index_names = load_neo4j_state()
    neo_node_status = {n["id"]: n["status"] for n in neo_nodes}
    neo_rel_key = {
        (r["rel_type"], r["from_id"], r["to_id"], r["cdi_id"]): r["status"]
        for r in neo_rels
    }
    print(f"  Neo4J nodes: {len(neo_node_status)}")
    print(f"  Neo4J relationships: {len(neo_rel_key)}")

    # ── Compute deltas ───────────────────────────────────────────────────────
    pg_node_ids = set(cdi_status.keys())
    neo_node_ids = set(neo_node_status.keys())

    nodes_to_insert = [
        {"id": cid, "core_id": cdi_meta[cid]["core_id"],
         "english_value": cdi_meta[cid]["english_value"],
         "status": cdi_status[cid]}
        for cid in pg_node_ids - neo_node_ids
    ]
    nodes_to_update_status = [
        {"id": cid, "status": cdi_status[cid]}
        for cid in pg_node_ids & neo_node_ids
        if neo_node_status[cid] != cdi_status[cid]
    ]
    nodes_to_delete = list(neo_node_ids - pg_node_ids)

    pg_rel_keys = {
        (r["rel_type"], r["from_id"], r["to_id"], r["cdi_id"]): r
        for r in rel_specs
    }
    rels_to_insert = [
        pg_rel_keys[k] for k in pg_rel_keys.keys() - neo_rel_key.keys()
    ]
    rels_to_update_status = [
        {**pg_rel_keys[k], "want": "ACTIVE"}
        for k in pg_rel_keys.keys() & neo_rel_key.keys()
        if neo_rel_key[k] != "ACTIVE"
    ]
    rels_to_delete = [
        {"rel_type": k[0], "from_id": k[1], "to_id": k[2], "cdi_id": k[3]}
        for k in neo_rel_key.keys() - pg_rel_keys.keys()
    ]

    # Discover the relationship types actually in use; index each on the
    # status + connect_id properties we'll filter on.
    with driver.session() as s:
        rel_types = sorted({
            r["t"] for r in s.run(
                "MATCH ()-[r]->() RETURN DISTINCT type(r) AS t"
            )
        })
    desired_indexes = [
        (f"rel_{rt.lower()}_{prop}", rt, prop)
        for rt in rel_types for prop in REL_INDEX_PROPS
    ]
    missing_indexes = [
        (name, rt, prop) for name, rt, prop in desired_indexes
        if name not in neo_index_names
    ]

    print()
    print("── Plan ─────────────────────────────────────────────────────────")
    print(f"  Nodes to INSERT:        {len(nodes_to_insert)}")
    print(f"  Nodes to UPDATE status: {len(nodes_to_update_status)}")
    print(f"  Nodes to DELETE:        {len(nodes_to_delete)}")
    print(f"  Rels to INSERT:         {len(rels_to_insert)}")
    print(f"  Rels to UPDATE status:  {len(rels_to_update_status)}")
    print(f"  Rels to DELETE:         {len(rels_to_delete)}")
    print(f"  Indexes to CREATE:      {len(missing_indexes)} "
          f"({', '.join(n for n, _, _ in missing_indexes) or 'none'})")

    if args.dry_run:
        print("\n(dry-run — no changes applied)")
        return

    # ── Apply ────────────────────────────────────────────────────────────────
    with driver.session() as s:
        if nodes_to_insert:
            for batch in chunked(nodes_to_insert, NEO4J_BATCH):
                s.run(
                    "UNWIND $rows AS row "
                    "MERGE (n:CoreDataItem {id: row.id}) "
                    "SET n.core_id = row.core_id, "
                    "    n.english_value = row.english_value, "
                    "    n.status = row.status",
                    rows=batch,
                )
            print(f"  ✓ Inserted {len(nodes_to_insert)} nodes")

        if nodes_to_update_status:
            for batch in chunked(nodes_to_update_status, NEO4J_BATCH):
                s.run(
                    "UNWIND $rows AS row "
                    "MATCH (n:CoreDataItem {id: row.id}) "
                    "SET n.status = row.status",
                    rows=batch,
                )
            print(f"  ✓ Updated status on {len(nodes_to_update_status)} nodes")

        if nodes_to_delete:
            for batch in chunked(nodes_to_delete, NEO4J_BATCH):
                s.run(
                    "UNWIND $ids AS nid "
                    "MATCH (n:CoreDataItem {id: nid}) DETACH DELETE n",
                    ids=batch,
                )
            print(f"  ✓ Deleted {len(nodes_to_delete)} orphan nodes")

        # Relationships are per-type — Neo4J needs the type literal in the
        # query, so we group by rel_type then emit one UNWIND per group.
        if rels_to_insert:
            by_type = defaultdict(list)
            for r in rels_to_insert:
                by_type[r["rel_type"]].append(r)
            for rt, group in by_type.items():
                for batch in chunked(group, NEO4J_BATCH):
                    s.run(
                        f"UNWIND $rows AS row "
                        f"MATCH (a:CoreDataItem {{id: row.from_id}}) "
                        f"MATCH (b:CoreDataItem {{id: row.to_id}}) "
                        f"MERGE (a)-[r:{rt} {{connect_data_item_id: row.cdi_id}}]->(b) "
                        f"SET r.connect_id = row.connect_id, "
                        f"    r.schema_position_from = row.from_pos, "
                        f"    r.schema_position_to = row.to_pos, "
                        f"    r.status = 'ACTIVE'",
                        rows=batch,
                    )
            print(f"  ✓ Inserted {len(rels_to_insert)} relationships")

        if rels_to_update_status:
            by_type = defaultdict(list)
            for r in rels_to_update_status:
                by_type[r["rel_type"]].append(r)
            for rt, group in by_type.items():
                for batch in chunked(group, NEO4J_BATCH):
                    s.run(
                        f"UNWIND $rows AS row "
                        f"MATCH (:CoreDataItem)-[r:{rt} "
                        f"  {{connect_data_item_id: row.cdi_id}}]->(:CoreDataItem) "
                        f"SET r.status = row.want",
                        rows=batch,
                    )
            print(f"  ✓ Updated status on {len(rels_to_update_status)} relationships")

        if rels_to_delete:
            by_type = defaultdict(list)
            for r in rels_to_delete:
                by_type[r["rel_type"]].append(r)
            for rt, group in by_type.items():
                for batch in chunked(group, NEO4J_BATCH):
                    s.run(
                        f"UNWIND $rows AS row "
                        f"MATCH (:CoreDataItem)-[r:{rt} "
                        f"  {{connect_data_item_id: row.cdi_id}}]->(:CoreDataItem) "
                        f"DELETE r",
                        rows=batch,
                    )
            print(f"  ✓ Deleted {len(rels_to_delete)} orphan relationships")

        for name, rt, prop in missing_indexes:
            s.run(
                f"CREATE INDEX {name} IF NOT EXISTS "
                f"FOR ()-[r:{rt}]-() ON (r.{prop})"
            )
            print(f"  ✓ Created index {name}  (on :{rt}.{prop})")

    print("\nDone.")


if __name__ == "__main__":
    main()
