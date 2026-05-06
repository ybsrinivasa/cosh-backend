#!/bin/bash
# git-pull-cosh2pro.sh — Clean pull of latest code from backend + frontend repos.
# Does NOT build or deploy — just refreshes the working trees so the next
# deploy-cosh2pro.sh sees fresh code.
#
# Run as coshpro (no sudo): bash deploy/scripts/git-pull-cosh2pro.sh
#
# Behaviour:
#   - Stashes any uncommitted local changes (auto-labelled, recoverable via `git stash list`)
#   - Fetches all branches and tags from origin
#   - Switches to the configured branch for each repo (BACKEND_BRANCH / FRONTEND_BRANCH below)
#   - Pulls fast-forward only — refuses merge commits or divergent local history
#   - Logs every step to a timestamped file under /data/cosh2.0Pro/backups/
#   - Prints incoming commits and file-change summary

set -euo pipefail

BACKEND_DIR=/data/cosh2.0Pro/cosh-backend
FRONTEND_DIR=/data/cosh2.0Pro/cosh-frontend

# Production branches (change here if you switch branches in future)
BACKEND_BRANCH=main
FRONTEND_BRANCH=main

LOG_DIR=/data/cosh2.0Pro/backups
LOG_FILE="$LOG_DIR/git-pull-$(date +%Y%m%d-%H%M%S).log"

mkdir -p "$LOG_DIR"

# Tee everything to the log file
exec > >(tee -a "$LOG_FILE") 2>&1

echo "=== Cosh 2.0 Pro — Git Pull $(date '+%Y-%m-%d %H:%M:%S') ==="
echo ""

pull_repo() {
    local NAME="$1"
    local DIR="$2"
    local TARGET_BRANCH="$3"

    echo "── $NAME ──────────────────────────────────────────"
    echo "Path:           $DIR"
    cd "$DIR"

    local CURRENT_BRANCH
    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    local BEFORE_SHA
    BEFORE_SHA=$(git rev-parse --short HEAD)
    echo "Current branch: $CURRENT_BRANCH @ $BEFORE_SHA"

    # Safety: stash any uncommitted local changes so checkout/pull don't fail
    if ! git diff --quiet || ! git diff --cached --quiet || \
       [ -n "$(git ls-files --others --exclude-standard 2>/dev/null)" ]; then
        echo ">> WARNING: uncommitted changes found. Stashing for safety..."
        git stash push --include-untracked \
            -m "auto-stash by git-pull-cosh2pro.sh $(date +%Y%m%d-%H%M%S)" || true
        echo "   (recoverable via:  cd $DIR && git stash list)"
    fi

    echo ""
    echo ">> Fetching from origin..."
    git fetch --all --tags --prune

    # Ensure local tracking branch exists, then switch
    if [ "$CURRENT_BRANCH" != "$TARGET_BRANCH" ]; then
        echo ""
        echo ">> Switching from '$CURRENT_BRANCH' to '$TARGET_BRANCH'..."
        if git show-ref --verify --quiet "refs/heads/$TARGET_BRANCH"; then
            git checkout "$TARGET_BRANCH"
        else
            git checkout -t "origin/$TARGET_BRANCH"
        fi
    fi

    # Show incoming commits
    local INCOMING
    INCOMING=$(git log --oneline "HEAD..origin/$TARGET_BRANCH" 2>/dev/null || echo "")

    echo ""
    if [ -z "$INCOMING" ]; then
        echo ">> Already up to date — no new commits."
    else
        echo ">> New commits to pull (oldest last):"
        echo "$INCOMING"
        echo ""
        echo ">> Pulling fast-forward only..."
        git pull --ff-only origin "$TARGET_BRANCH"

        local AFTER_SHA
        AFTER_SHA=$(git rev-parse --short HEAD)
        echo ""
        echo ">> $NAME advanced: $BEFORE_SHA → $AFTER_SHA"

        echo ""
        echo ">> Files changed:"
        git diff --stat "$BEFORE_SHA..$AFTER_SHA"
    fi

    echo ""
}

pull_repo "Backend"  "$BACKEND_DIR"  "$BACKEND_BRANCH"
pull_repo "Frontend" "$FRONTEND_DIR" "$FRONTEND_BRANCH"

echo "=== Pull complete ==="
echo ""
echo "Log file: $LOG_FILE"
echo ""
echo "Next step (when you're ready to apply):"
echo "  cd $BACKEND_DIR"
echo "  bash deploy/scripts/deploy-cosh2pro.sh"
