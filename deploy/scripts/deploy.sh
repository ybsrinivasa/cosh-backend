#!/bin/bash
# deploy.sh — Pull latest code, rebuild, migrate, restart
# Run as: bash deploy.sh
# Safe to run repeatedly. Neo4J is never restarted.

set -euo pipefail

BACKEND_DIR=/data/cosh2.0/cosh-backend
FRONTEND_DIR=/data/cosh2.0/cosh-frontend

echo "=== Cosh 2.0 — Deploy $(date '+%Y-%m-%d %H:%M:%S') ==="

# ── Pull latest code ─────────────────────────────────────────────────────────
echo ">> Pulling latest backend..."
git -C "$BACKEND_DIR" pull origin main

echo ">> Pulling latest frontend..."
git -C "$FRONTEND_DIR" pull origin main

# ── Build and restart app services ───────────────────────────────────────────
cd "$BACKEND_DIR"

echo ">> Building images..."
docker compose -f docker-compose.prod.yml build api celery frontend

echo ">> Starting infrastructure (postgres, redis, neo4j) if not running..."
docker compose -f docker-compose.prod.yml up -d postgres redis neo4j

echo ">> Waiting for PostgreSQL to be healthy..."
until docker compose -f docker-compose.prod.yml exec -T postgres \
    pg_isready -U "$(grep POSTGRES_USER .env | cut -d= -f2)" 2>/dev/null; do
    echo "   waiting..."
    sleep 3
done

echo ">> Running database migrations..."
docker compose -f docker-compose.prod.yml run --rm api \
    alembic upgrade head

echo ">> Restarting API and Celery workers..."
docker compose -f docker-compose.prod.yml up -d --force-recreate api celery

echo ">> Restarting frontend..."
docker compose -f docker-compose.prod.yml up -d --force-recreate frontend

echo ">> Cleaning up old images..."
docker image prune -f

echo ""
echo "=== Deploy complete ==="
echo "   API health:     $(curl -s http://localhost:8000/health)"
echo "   Frontend:       http://localhost:3000"
echo "   Public URL:     https://cosh.dev.eywa.farm"
