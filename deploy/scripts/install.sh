#!/bin/bash
# install.sh — One-time setup for Stage 1 EC2 (Ubuntu 22.04)
# Run as: sudo bash install.sh
# Run ONCE after the EC2 instance is provisioned and EBS volumes are attached.

set -euo pipefail

echo "=== Cosh 2.0 Stage 1 — Server Setup ==="

# ── System packages ──────────────────────────────────────────────────────────
apt-get update
apt-get install -y \
    ca-certificates curl gnupg lsb-release \
    nginx certbot python3-certbot-nginx \
    git awscli unzip

# ── Docker ───────────────────────────────────────────────────────────────────
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
  > /etc/apt/sources.list.d/docker.list
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Add ubuntu user to docker group (no sudo needed for docker commands)
usermod -aG docker newroot

# ── EBS volume mounts ────────────────────────────────────────────────────────
# Assumes EBS volumes are attached. Format and mount if not already done.
# Check your EC2 console for the device names (usually /dev/xvdf, /dev/xvdg)

echo ""
echo ">>> ACTION REQUIRED: EBS volumes must be formatted and mounted."
echo "    Run these commands for each new (blank) EBS volume:"
echo ""
echo "    # For the 20 GB PostgreSQL volume (e.g. /dev/xvdf):"
echo "    sudo mkfs.ext4 /dev/xvdf"
echo "    sudo mkdir -p /data/postgres"
echo "    sudo mount /dev/xvdf /data/postgres"
echo "    echo '/dev/xvdf /data/postgres ext4 defaults,nofail 0 2' | sudo tee -a /etc/fstab"
echo ""
echo "    # For the 50 GB Neo4J volume (e.g. /dev/xvdg):"
echo "    sudo mkfs.ext4 /dev/xvdg"
echo "    sudo mkdir -p /data/neo4j/data /data/neo4j/logs /data/neo4j/plugins"
echo "    sudo mount /dev/xvdg /data/neo4j"
echo "    echo '/dev/xvdg /data/neo4j ext4 defaults,nofail 0 2' | sudo tee -a /etc/fstab"
echo ""

# Create directories (safe to run even before EBS mount, useful for testing)
mkdir -p /data/postgres
mkdir -p /data/neo4j/data /data/neo4j/logs /data/neo4j/plugins

# Fix ownership so Docker can write
chown -R 1000:1000 /data/neo4j
chown -R 999:999 /data/postgres   # postgres Docker image uses uid 999

# ── Clone repositories ───────────────────────────────────────────────────────
cd /home/ubuntu

if [ ! -d "cosh-backend" ]; then
    git clone https://github.com/ybsrinivasa/cosh-backend.git
fi

if [ ! -d "cosh-frontend" ]; then
    git clone https://github.com/ybsrinivasa/cosh-frontend.git
fi

chown -R newroot:newroot /data/cosh2.0/cosh-backend /data/cosh2.0/cosh-frontend

# ── Nginx config ─────────────────────────────────────────────────────────────
cp /data/cosh2.0/cosh-backend/deploy/nginx.conf /etc/nginx/sites-available/cosh
ln -sf /etc/nginx/sites-available/cosh /etc/nginx/sites-enabled/cosh
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl reload nginx

# ── Neo4J backup cron ────────────────────────────────────────────────────────
cp /data/cosh2.0/cosh-backend/deploy/scripts/neo4j-backup.sh /home/newroot/neo4j-backup.sh
chmod +x /home/newroot/neo4j-backup.sh
chown newroot:newroot /home/newroot/neo4j-backup.sh

# Add daily 2 AM cron job for neo4j backup
(crontab -u newroot -l 2>/dev/null; echo "0 2 * * * /home/newroot/neo4j-backup.sh >> /var/log/neo4j-backup.log 2>&1") | crontab -u newroot -

echo ""
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "  1. Mount EBS volumes (see instructions above)"
echo "  2. Copy deploy/.env.stage1.example → /data/cosh2.0/cosh-backend/.env and fill in values"
echo "  3. Run: sudo bash /data/cosh2.0/cosh-backend/deploy/scripts/deploy.sh"
echo "  4. Point DNS: cosh.dev.eywa.farm → this server's Elastic IP"
echo "  5. Run: sudo certbot --nginx -d cosh.dev.eywa.farm"
echo "  6. Run: bash /data/cosh2.0/cosh-backend/deploy/scripts/neo4j-init.sh"
