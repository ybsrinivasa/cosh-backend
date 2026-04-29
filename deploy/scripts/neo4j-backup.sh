#!/bin/bash
# neo4j-backup.sh — Daily Neo4J dump to S3
# Cron: 0 2 * * * /home/newroot/neo4j-backup.sh >> /var/log/neo4j-backup.log 2>&1

set -euo pipefail

BACKEND_DIR=/data/cosh2.0/cosh-backend
DATE=$(date +%Y-%m-%d)
DUMP_PATH=/data/neo4j-backup
S3_BUCKET=tene-drs-prod-media          # reuse media bucket; backups go in a separate prefix
S3_PREFIX=cosh-dev-backups/neo4j
RETENTION_DAYS=14

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting Neo4J backup..."

# ── Dump ─────────────────────────────────────────────────────────────────────
mkdir -p "$DUMP_PATH"

docker compose -f "$BACKEND_DIR/docker-compose.prod.yml" exec -T neo4j \
    neo4j-admin database dump neo4j --to-path=/data/neo4j-backup --overwrite-destination=true

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Dump complete. Uploading to S3..."

# ── Upload ────────────────────────────────────────────────────────────────────
aws s3 cp "$DUMP_PATH/neo4j.dump" \
    "s3://${S3_BUCKET}/${S3_PREFIX}/neo4j-${DATE}.dump"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Upload complete: s3://${S3_BUCKET}/${S3_PREFIX}/neo4j-${DATE}.dump"

# ── Retention: remove dumps older than RETENTION_DAYS ────────────────────────
aws s3 ls "s3://${S3_BUCKET}/${S3_PREFIX}/" \
    | awk '{print $4}' \
    | sort \
    | head -n "-${RETENTION_DAYS}" \
    | xargs -r -I {} aws s3 rm "s3://${S3_BUCKET}/${S3_PREFIX}/{}"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Backup complete. Retention: last ${RETENTION_DAYS} days kept."
