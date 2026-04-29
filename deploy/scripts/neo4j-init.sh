#!/bin/bash
# neo4j-init.sh — Create constraints and full-text index in Neo4J
# Run ONCE after Neo4J starts for the first time on a fresh database.
# Safe to re-run: CREATE CONSTRAINT IF NOT EXISTS / CREATE INDEX IF NOT EXISTS

set -euo pipefail

BACKEND_DIR=/data/cosh2.0/cosh-backend
NEO4J_PASSWORD=$(grep NEO4J_PASSWORD "$BACKEND_DIR/.env" | cut -d= -f2)

echo "=== Neo4J Schema Init ==="

run_cypher() {
    docker compose -f "$BACKEND_DIR/docker-compose.prod.yml" exec -T neo4j \
        cypher-shell -u neo4j -p "$NEO4J_PASSWORD" "$1"
}

echo ">> Creating node uniqueness constraint..."
run_cypher "CREATE CONSTRAINT cdi_id IF NOT EXISTS FOR (n:CoreDataItem) REQUIRE n.id IS UNIQUE;"

echo ">> Creating full-text index for similarity search..."
run_cypher "CREATE FULLTEXT INDEX cdi_english IF NOT EXISTS FOR (n:CoreDataItem) ON EACH [n.english_value];"

echo ">> Verifying indexes..."
run_cypher "CALL db.indexes() YIELD name, type, state RETURN name, type, state;"

echo ""
echo "=== Neo4J init complete ==="
