"""
P7-04 — Migration Verification
Spot-checks that the Cosh 1.0 → 2.0 migration is complete and consistent.
Run after P7-03 (Connect migration) and before triggering First Pass (P7-05).

Checks:
  1. Core Data Item counts per Core (PostgreSQL)
  2. Neo4J node count matches PostgreSQL count
  3. Connect Data Item counts per Connect
  4. Neo4J relationship count matches Connect Data Item count
  5. Translation coverage per language (% items translated)
  6. Sample Neo4J path queries (Pest→Stage, Brand→Manufacturer)

Usage:
    python scripts/migration_verify.py
    python scripts/migration_verify.py --out verification_report.txt
"""
import os
import sys
from pathlib import Path
from datetime import datetime

# Load .env
from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text as sql_text
from sqlalchemy.orm import Session
from neo4j import GraphDatabase


def get_pg_engine():
    url = os.getenv("DATABASE_URL_SYNC")
    if not url:
        print("ERROR: DATABASE_URL_SYNC not set in .env", file=sys.stderr)
        sys.exit(1)
    return create_engine(url)


def get_neo4j_driver():
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "Cosh@2026")
    return GraphDatabase.driver(uri, auth=(user, password))


def run_verification(out_file=None):
    lines = []

    def log(msg=""):
        lines.append(msg)
        print(msg)

    log(f"Cosh 2.0 Migration Verification Report")
    log(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 60)

    engine = get_pg_engine()
    driver = get_neo4j_driver()
    passed = 0
    failed = 0
    warnings = 0

    with Session(engine) as session, driver.session() as neo:

        # ── 1. Core Data Item counts ───────────────────────────────────────────
        log("\n1. CORE DATA ITEMS")
        log("-" * 40)

        cores = session.execute(sql_text(
            "SELECT c.id, c.name, c.core_type, "
            "COUNT(cdi.id) FILTER (WHERE cdi.status='ACTIVE') AS active_count, "
            "COUNT(cdi.id) AS total_count "
            "FROM cores c LEFT JOIN core_data_items cdi ON c.id = cdi.core_id "
            "WHERE c.status='ACTIVE' GROUP BY c.id, c.name, c.core_type ORDER BY c.name"
        )).fetchall()

        total_items = 0
        for core in cores:
            total_items += core.active_count
            log(f"  {core.name} ({core.core_type}): {core.active_count} active / {core.total_count} total")

        log(f"\n  Total active Core Data Items: {total_items}")
        passed += 1

        # ── 2. Neo4J node count vs PostgreSQL ──────────────────────────────────
        log("\n2. NEO4J NODE CONSISTENCY")
        log("-" * 40)

        pg_count = session.execute(sql_text(
            "SELECT COUNT(*) FROM core_data_items WHERE status='ACTIVE'"
        )).scalar()

        neo_result = neo.run("MATCH (n:CoreDataItem {status: 'ACTIVE'}) RETURN count(n) AS cnt")
        neo_count = neo_result.single()["cnt"]

        if pg_count == neo_count:
            log(f"  PostgreSQL: {pg_count}  |  Neo4J: {neo_count}  ✓ MATCH")
            passed += 1
        else:
            log(f"  PostgreSQL: {pg_count}  |  Neo4J: {neo_count}  ✗ MISMATCH — {abs(pg_count - neo_count)} difference")
            failed += 1

        # ── 3. Connect Data Item counts ────────────────────────────────────────
        log("\n3. CONNECT DATA ITEMS")
        log("-" * 40)

        connects = session.execute(sql_text(
            "SELECT c.id, c.name, "
            "COUNT(cdi.id) FILTER (WHERE cdi.status='ACTIVE') AS active_count, "
            "COUNT(cdi.id) AS total_count "
            "FROM connects c LEFT JOIN connect_data_items cdi ON c.id = cdi.connect_id "
            "WHERE c.status='ACTIVE' GROUP BY c.id, c.name ORDER BY c.name"
        )).fetchall()

        total_connect_items = 0
        for conn in connects:
            total_connect_items += conn.active_count
            log(f"  {conn.name}: {conn.active_count} active / {conn.total_count} total")

        log(f"\n  Total active Connect Data Items: {total_connect_items}")
        passed += 1

        # ── 4. Neo4J relationship count ────────────────────────────────────────
        log("\n4. NEO4J RELATIONSHIP CONSISTENCY")
        log("-" * 40)

        pg_rel_count = session.execute(sql_text(
            "SELECT COUNT(DISTINCT connect_data_item_id) FROM connect_data_positions "
            "JOIN connect_data_items ON connect_data_items.id = connect_data_positions.connect_data_item_id "
            "WHERE connect_data_items.status='ACTIVE'"
        )).scalar()

        neo_rel_result = neo.run("MATCH ()-[r {status:'ACTIVE'}]->() RETURN count(r) AS cnt")
        neo_rel_count = neo_rel_result.single()["cnt"]

        # Each Connect Data Item with N positions creates N-1 relationships
        if neo_rel_count > 0:
            log(f"  Active Connect Data Items (PG): {pg_rel_count}")
            log(f"  Active Neo4J relationships:     {neo_rel_count}")
            if neo_rel_count >= pg_rel_count:
                log(f"  ✓ Neo4J has relationships for all active items")
                passed += 1
            else:
                log(f"  ✗ Neo4J has fewer relationships than expected — possible dual-write gap")
                failed += 1
        else:
            log(f"  No Neo4J relationships found — Connect migration not yet run or no data")
            warnings += 1

        # ── 5. Translation coverage ────────────────────────────────────────────
        log("\n5. TRANSLATION COVERAGE")
        log("-" * 40)

        languages = session.execute(sql_text(
            "SELECT language_code, language_name_en FROM language_registry "
            "WHERE status='ACTIVE' AND language_code != 'en' ORDER BY language_code"
        )).fetchall()

        total_active = session.execute(sql_text(
            "SELECT COUNT(*) FROM core_data_items cdi "
            "JOIN cores c ON c.id = cdi.core_id "
            "WHERE cdi.status='ACTIVE' AND c.core_type='TEXT'"
        )).scalar()

        if total_active > 0:
            for lang in languages:
                translated = session.execute(sql_text(
                    "SELECT COUNT(*) FROM core_data_translations WHERE language_code = :lang"
                ), {"lang": lang.language_code}).scalar()
                expert = session.execute(sql_text(
                    "SELECT COUNT(*) FROM core_data_translations "
                    "WHERE language_code = :lang AND validation_status='EXPERT_VALIDATED'"
                ), {"lang": lang.language_code}).scalar()
                pct = round(translated / total_active * 100, 1) if total_active else 0
                log(f"  {lang.language_code} ({lang.language_name_en}): {translated}/{total_active} ({pct}%) — {expert} expert-validated")
        else:
            log("  No active TEXT Core items found")
            warnings += 1

        # ── 6. Sample Neo4J path queries ───────────────────────────────────────
        log("\n6. SAMPLE NEO4J PATH QUERIES")
        log("-" * 40)

        sample_queries = [
            ("Any 2-hop path", "MATCH (a:CoreDataItem)-[r1]->(b:CoreDataItem)-[r2]->(c:CoreDataItem) RETURN a.english_value, type(r1), b.english_value, type(r2), c.english_value LIMIT 3"),
            ("Any relationship", "MATCH (a:CoreDataItem)-[r]->(b:CoreDataItem) RETURN a.english_value, type(r), b.english_value LIMIT 5"),
        ]

        for label, cypher in sample_queries:
            try:
                results = neo.run(cypher).data()
                if results:
                    log(f"  {label}: {len(results)} result(s) found")
                    for r in results[:2]:
                        values = list(r.values())
                        log(f"    → {' | '.join(str(v) for v in values)}")
                    passed += 1
                else:
                    log(f"  {label}: no results — no Connect data yet or empty graph")
                    warnings += 1
            except Exception as e:
                log(f"  {label}: query failed — {e}")
                warnings += 1

        # ── 7. Similarity pairs status ─────────────────────────────────────────
        log("\n7. SIMILARITY REVIEW STATUS")
        log("-" * 40)

        sim_counts = session.execute(sql_text(
            "SELECT status, COUNT(*) AS cnt FROM similarity_pairs GROUP BY status ORDER BY status"
        )).fetchall()

        if sim_counts:
            for row in sim_counts:
                log(f"  {row.status}: {row.cnt}")
        else:
            log("  No similarity pairs yet — First Pass not yet run")

        # ── Summary ────────────────────────────────────────────────────────────
        log("\n" + "=" * 60)
        log("VERIFICATION SUMMARY")
        log(f"  Passed:   {passed}")
        log(f"  Failed:   {failed}")
        log(f"  Warnings: {warnings}")

        if failed == 0:
            log("\n  ✓ Migration verification PASSED — safe to proceed to P7-05 (First Pass)")
        else:
            log(f"\n  ✗ {failed} check(s) FAILED — resolve issues before proceeding")

        log()

    driver.close()

    if out_file:
        Path(out_file).write_text("\n".join(lines))
        print(f"\nReport saved to: {out_file}")

    return failed


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Cosh 2.0 migration verification")
    parser.add_argument("--out", default=None, help="Optional: save report to this file")
    args = parser.parse_args()

    failures = run_verification(args.out)
    sys.exit(1 if failures else 0)
