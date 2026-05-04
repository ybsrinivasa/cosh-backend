#!/bin/bash
# backup-cosh2pro.sh — Daily backup of PostgreSQL → S3.
# Add to coshpro's crontab:
#   0 2 * * * /data/cosh2.0Pro/cosh-backend/deploy/scripts/backup-cosh2pro.sh >> /data/cosh2.0Pro/backups/backup.log 2>&1

set -euo pipefail

BACKEND_DIR=/data/cosh2.0Pro/cosh-backend
BACKUP_DIR=/data/cosh2.0Pro/backups
COMPOSE="docker compose -f docker-compose.cosh2pro.yml"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
DUMP_FILE="$BACKUP_DIR/cosh_prod-$TIMESTAMP.sql.gz"

cd "$BACKEND_DIR"

# Read env values
POSTGRES_USER=$(grep ^POSTGRES_USER .env | cut -d= -f2)
POSTGRES_DB=$(grep ^POSTGRES_DB .env | cut -d= -f2)
S3_BUCKET=$(grep ^S3_BUCKET_MEDIA .env | cut -d= -f2)

# ── 1. Postgres dump ─────────────────────────────────────────────────────────
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Dumping postgres..."
$COMPOSE exec -T postgres \
    pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --no-owner --no-acl \
    | gzip > "$DUMP_FILE"

SIZE=$(du -h "$DUMP_FILE" | cut -f1)
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Dump created: $DUMP_FILE ($SIZE)"

# ── 2. Upload to S3 ──────────────────────────────────────────────────────────
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Uploading to S3..."
aws s3 cp "$DUMP_FILE" "s3://$S3_BUCKET/cosh2-prod/backups/postgres/$(basename "$DUMP_FILE")"

# ── 3. Local retention: keep last 7 days only ────────────────────────────────
find "$BACKUP_DIR" -name 'cosh_prod-*.sql.gz' -mtime +7 -delete

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Backup complete."
