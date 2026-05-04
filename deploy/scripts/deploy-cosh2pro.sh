#!/bin/bash
# deploy-cosh2pro.sh — Pull latest, build, migrate, seed, restart.
# Run as coshpro (no sudo): bash deploy/scripts/deploy-cosh2pro.sh
# Safe to re-run.

set -euo pipefail

BACKEND_DIR=/data/cosh2.0Pro/cosh-backend
FRONTEND_DIR=/data/cosh2.0Pro/cosh-frontend
COMPOSE="docker compose -f docker-compose.cosh2pro.yml"

echo "=== Cosh 2.0 Pro — Deploy $(date '+%Y-%m-%d %H:%M:%S') ==="

cd "$BACKEND_DIR"

# ── 1. Pre-flight checks ─────────────────────────────────────────────────────
if [ ! -f .env ]; then
    echo "ERROR: .env not found in $BACKEND_DIR"
    echo "Run: cp .env.cosh2pro.example .env  and edit with real secrets first"
    exit 1
fi

if ! grep -q "^SECRET_KEY=" .env || grep -q "CHANGE_ME" .env; then
    echo "ERROR: .env still has placeholder values. Edit it first."
    grep "CHANGE_ME" .env || true
    exit 1
fi

# Ensure POSTGRES_PASSWORD value appears inside DATABASE_URL and DATABASE_URL_SYNC.
# (One morning we lost an hour because postgres was init'd with one password and
# the app's connection string had another. Catch this BEFORE postgres starts.)
PG_PWD=$(grep '^POSTGRES_PASSWORD=' .env | cut -d= -f2-)
if [ -z "$PG_PWD" ]; then
    echo "ERROR: POSTGRES_PASSWORD is empty in .env"
    exit 1
fi
for var in DATABASE_URL DATABASE_URL_SYNC; do
    URL=$(grep "^${var}=" .env | cut -d= -f2-)
    if [ -z "$URL" ]; then
        echo "ERROR: ${var} not set in .env"
        exit 1
    fi
    # The URL must contain :PASSWORD@ to authenticate as the cosh user
    if ! echo "$URL" | grep -qF ":${PG_PWD}@"; then
        echo "ERROR: ${var} does not contain POSTGRES_PASSWORD."
        echo "       All three values must use the same password:"
        echo "       POSTGRES_PASSWORD, DATABASE_URL, DATABASE_URL_SYNC"
        exit 1
    fi
done

# ── 2. Pull latest code ──────────────────────────────────────────────────────
echo ">> Pulling latest backend..."
git -C "$BACKEND_DIR" pull origin main

echo ">> Pulling latest frontend..."
git -C "$FRONTEND_DIR" pull origin main

# ── 3. Build images ──────────────────────────────────────────────────────────
echo ">> Building images..."
$COMPOSE build api celery celery-beat frontend

# ── 4. Start infrastructure ──────────────────────────────────────────────────
echo ">> Starting postgres, redis, neo4j..."
$COMPOSE up -d postgres redis neo4j

echo ">> Waiting for PostgreSQL..."
until $COMPOSE exec -T postgres pg_isready -U "$(grep ^POSTGRES_USER .env | cut -d= -f2)" 2>/dev/null; do
    echo "   waiting..."
    sleep 3
done

# ── 5. Migrations + seed ─────────────────────────────────────────────────────
echo ">> Running database migrations..."
$COMPOSE run --rm api alembic upgrade head

if [ -f scripts/seed_db.py ]; then
    echo ">> Seeding database (idempotent)..."
    $COMPOSE run --rm api python scripts/seed_db.py
fi

# ── 6. Start app services ────────────────────────────────────────────────────
echo ">> Restarting API, Celery, frontend..."
$COMPOSE up -d --force-recreate api celery celery-beat frontend

# ── 7. Cleanup ───────────────────────────────────────────────────────────────
echo ">> Pruning old images..."
docker image prune -f

# ── 8. Smoke test ────────────────────────────────────────────────────────────
echo ""
echo ">> Waiting 10s for services to settle..."
sleep 10
echo "API health (origin):  $(curl -fsS http://127.0.0.1:8000/health 2>&1 | head -c 200)"
echo "Origin via nginx:     $(curl -fsS http://127.0.0.1:3860/health 2>&1 | head -c 200)"

echo ""
echo "=== Deploy complete ==="
echo "Public:  https://cosh2.eywa.farm"
echo "LAN:     http://10.12.10.115:3860"
echo ""
echo "Logs:    $COMPOSE logs -f api"
