#!/bin/bash
# neo4j-init.sh — Create constraints and full-text index in Neo4J
# Run ONCE after Neo4J starts for the first time on a fresh database.
# Safe to re-run: CREATE CONSTRAINT IF NOT EXISTS / CREATE INDEX IF NOT EXISTS

set -euo pipefail

BACKEND_DIR=/data/cosh2.0/cosh-backend
NEO4J_PASSWORD=$(grep NEO4J_PASSWORD "$BACKEND_DIR/.env" | cut -d= -f2)

echo "=== Neo4J Schema Init ==="

# Wait for Neo4J container to be fully running (not restarting)
echo ">> Waiting for Neo4J container to be ready..."
for i in $(seq 1 30); do
    STATUS=$(docker compose -f "$BACKEND_DIR/docker-compose.prod.yml" ps --format json neo4j 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d[0]['State'] if isinstance(d,list) else d['State'])" 2>/dev/null || echo "unknown")
    if [ "$STATUS" = "running" ]; then
        echo "   Neo4J is running."
        break
    fi
    echo "   Status: $STATUS — waiting 10s ($i/30)..."
    sleep 10
done

# Wait a further 30s for bolt to be fully ready inside the container
echo ">> Waiting 30s for Bolt to initialise..."
sleep 30

run_cypher() {
    docker compose -f "$BACKEND_DIR/docker-compose.prod.yml" exec -T neo4j \
        cypher-shell -u neo4j -p "$NEO4J_PASSWORD" "$1"
}

echo ">> Creating node uniqueness constraint..."
run_cypher "CREATE CONSTRAINT cdi_id IF NOT EXISTS FOR (n:CoreDataItem) REQUIRE n.id IS UNIQUE;"

echo ">> Creating full-text index for similarity search..."
run_cypher "CREATE FULLTEXT INDEX cdi_english IF NOT EXISTS FOR (n:CoreDataItem) ON EACH [n.english_value];"

echo ">> Verifying indexes..."
run_cypher "SHOW INDEXES YIELD name, type, state RETURN name, type, state;"

echo ""
echo "=== Neo4J init complete ==="
