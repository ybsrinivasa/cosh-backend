#!/bin/bash
# install-cosh2pro.sh — One-time setup for the local production server.
# Server: Ubuntu 22.04 LTS at 10.12.10.115:3860 (public 106.51.66.90:3860)
# DNS:    https://cosh2.eywa.farm (SSL terminated at Cloudflare)
# Run as: sudo bash install-cosh2pro.sh
# Safe to re-run.

set -euo pipefail

echo "=== Cosh 2.0 Pro — Server install ==="

# ── 1. System packages ────────────────────────────────────────────────────────
echo ">> Installing system packages..."
apt-get update
apt-get install -y \
    ca-certificates curl gnupg lsb-release \
    nginx git awscli unzip jq

# ── 2. Docker + Compose plugin ───────────────────────────────────────────────
if ! command -v docker &>/dev/null; then
    echo ">> Installing Docker..."
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
        | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    chmod a+r /etc/apt/keyrings/docker.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
      https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
      > /etc/apt/sources.list.d/docker.list
    apt-get update
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
fi

# Add coshpro to docker group (no sudo needed for docker commands after re-login)
usermod -aG docker coshpro

# ── 3. Data directories ──────────────────────────────────────────────────────
echo ">> Creating data directories..."
mkdir -p /data/cosh2.0Pro/db/postgres
mkdir -p /data/cosh2.0Pro/db/neo4j/data
mkdir -p /data/cosh2.0Pro/db/neo4j/logs
mkdir -p /data/cosh2.0Pro/db/neo4j/plugins
mkdir -p /data/cosh2.0Pro/backups

# Ownership for Docker volumes: postgres image runs as uid 999, Neo4J as uid 7474
chown -R 999:999 /data/cosh2.0Pro/db/postgres
chown -R 7474:7474 /data/cosh2.0Pro/db/neo4j

# Ownership for repos (already cloned but root-owned — fix it)
chown -R coshpro:coshpro /data/cosh2.0Pro/cosh-backend
chown -R coshpro:coshpro /data/cosh2.0Pro/cosh-frontend
chown -R coshpro:coshpro /data/cosh2.0Pro/backups

# ── 4. Nginx site ────────────────────────────────────────────────────────────
echo ">> Configuring nginx..."
cp /data/cosh2.0Pro/cosh-backend/deploy/nginx-cosh2pro.conf /etc/nginx/sites-available/cosh2pro
ln -sf /etc/nginx/sites-available/cosh2pro /etc/nginx/sites-enabled/cosh2pro
rm -f /etc/nginx/sites-enabled/default

nginx -t
systemctl enable nginx
systemctl reload nginx

# ── 5. Firewall (ufw) ────────────────────────────────────────────────────────
# Open SSH (22) and the app port (3860) only. Postgres/Neo4J/Redis stay internal.
echo ">> Configuring firewall..."
if command -v ufw &>/dev/null; then
    ufw allow 22/tcp
    ufw allow 3860/tcp
    ufw --force enable
    ufw status
fi

# ── 6. Done ──────────────────────────────────────────────────────────────────
echo ""
echo "=== Install complete ==="
echo ""
echo "Next steps:"
echo "  1. Log out and SSH back in (so coshpro picks up docker group membership)"
echo "  2. cd /data/cosh2.0Pro/cosh-backend"
echo "  3. cp .env.cosh2pro.example .env  # then edit with real secrets"
echo "  4. bash deploy/scripts/deploy-cosh2pro.sh"
echo ""
echo "After deploy, verify:"
echo "  - http://10.12.10.115:3860/health  (LAN test)"
echo "  - http://106.51.66.90:3860/health  (public IP test)"
echo "  - https://cosh2.eywa.farm/         (full DNS + SSL test)"
