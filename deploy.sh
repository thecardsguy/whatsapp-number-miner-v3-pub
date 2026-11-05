#!/bin/bash
# ============================================================
# WhatsApp Number Miner — One-Command Deploy Script
# Run this from the whatsapp-number-miner/ folder
# ============================================================
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}  WhatsApp Number Miner — Deploy Script${NC}"
echo -e "${GREEN}============================================${NC}"
echo ""

# ── Step 1: Check prerequisites ────────────────────────────
echo -e "${YELLOW}[1/4] Checking prerequisites...${NC}"

if ! command -v git &> /dev/null; then
    echo -e "${RED}git is not installed. Install it first: https://git-scm.com${NC}"
    exit 1
fi

if ! command -v gh &> /dev/null; then
    echo -e "${YELLOW}GitHub CLI (gh) not found. You'll need to create the repo manually.${NC}"
    echo -e "${YELLOW}Install it: brew install gh   or   apt install gh${NC}"
    echo -e "${YELLOW}Then run: gh auth login${NC}"
    echo ""
    HAS_GH=false
else
    HAS_GH=true
    echo -e "${GREEN}✓ git and gh CLI found${NC}"
fi

# ── Step 2: Initialize git and push ────────────────────────
echo ""
echo -e "${YELLOW}[2/4] Setting up git repo...${NC}"

if [ ! -d .git ]; then
    git init
    git add .
    git commit -m "WhatsApp Number Miner v3.1 — production-ready"
    echo -e "${GREEN}✓ Git repo initialized${NC}"
else
    echo -e "${GREEN}✓ Git repo already exists${NC}"
fi

if [ "$HAS_GH" = true ]; then
    echo ""
    echo -e "${YELLOW}Creating GitHub repo and pushing...${NC}"
    
    # Check if remote already exists
    if git remote get-url origin &> /dev/null; then
        echo -e "${GREEN}✓ Remote origin already set${NC}"
        git push -u origin main 2>/dev/null || git push -u origin master
    else
        # Create repo and push
        gh repo create whatsapp-number-miner --public --source=. --remote=origin --push
        echo -e "${GREEN}✓ Repo created and code pushed!${NC}"
    fi
    
    REPO_URL=$(git remote get-url origin 2>/dev/null | sed 's/\.git$//' | sed 's/^git@github\.com:/https:\/\/github.com\//')
else
    echo ""
    echo -e "${YELLOW}Manual steps (copy-paste these):${NC}"
    echo ""
    echo "  1. Go to: https://github.com/new"
    echo "  2. Name: whatsapp-number-miner"
    echo "  3. Don't initialize with README"
    echo "  4. Click 'Create repository'"
    echo "  5. Then run:"
    echo ""
    echo "     git remote add origin https://github.com/YOUR_USERNAME/whatsapp-number-miner.git"
    echo "     git branch -M main"
    echo "     git push -u origin main"
    echo ""
    read -p "Press Enter after you've pushed to GitHub..."
    REPO_URL=""
fi

# ── Step 3: Render deploy instructions ─────────────────────
echo ""
echo -e "${YELLOW}[3/4] Deploy to Render...${NC}"
echo ""

# Generate a random password
RANDOM_PW=$(head -c 12 /dev/urandom 2>/dev/null | base64 | tr -d '+/=' | head -c 12 || echo "changeme123")
GOWA_PW=$(head -c 12 /dev/urandom 2>/dev/null | base64 | tr -d '+/=' | head -c 12 || echo "gowapass123")

echo -e "${GREEN}Your auto-generated passwords (save these!):${NC}"
echo -e "  Dashboard password: ${YELLOW}${RANDOM_PW}${NC}"
echo -e "  GOWA API password:  ${YELLOW}${GOWA_PW}${NC}"
echo ""

# Create a deploy URL for Render
if [ -n "$REPO_URL" ]; then
    DEPLOY_URL="https://dashboard.render.com/select-repo?type=web&repo=$(echo "$REPO_URL" | sed 's/https:\/\/github\.com\///')"
    echo -e "${GREEN}One-click deploy link:${NC}"
    echo -e "  ${DEPLOY_URL}"
    echo ""
fi

echo -e "Or manually:"
echo -e "  1. Go to: ${YELLOW}https://dashboard.render.com${NC}"
echo -e "  2. Click ${YELLOW}New → Web Service${NC}"
echo -e "  3. Connect your GitHub repo"
echo -e "  4. Render auto-detects render.yaml"
echo -e "  5. Add these environment variables:"
echo ""
echo -e "     ${GREEN}APP_PASSWORD${NC}     = ${YELLOW}${RANDOM_PW}${NC}"
echo -e "     ${GREEN}GOWA_BASIC_AUTH${NC}  = ${YELLOW}admin:${GOWA_PW}${NC}"
echo -e "     ${GREEN}DATABASE_URL${NC}     = ${YELLOW}sqlite:////app/data/miner.db${NC}"
echo -e "     ${GREEN}TARGET_NPA${NC}       = ${YELLOW}(leave empty)${NC}"
echo ""
echo -e "  6. Click ${YELLOW}Create Web Service${NC}"
echo ""

# ── Step 4: Keep-alive cron ──���──────────────────────────────
echo -e "${YELLOW}[4/4] After deploy, keep it alive:${NC}"
echo ""
echo -e "  1. Go to: ${YELLOW}https://cron-job.org${NC}"
echo -e "  2. Create cron job → URL: ${YELLOW}https://YOUR-APP.onrender.com/health${NC}"
echo -e "  3. Interval: ${YELLOW}Every 10 minutes${NC}"
echo ""

# ── Save passwords ─────────────────────────────────────────
cat > .env.deploy <<EOF
# WhatsApp Number Miner — Deployment Credentials
# Generated $(date)
APP_PASSWORD=${RANDOM_PW}
GOWA_BASIC_AUTH=admin:${GOWA_PW}
DATABASE_URL=sqlite:////app/data/miner.db
EOF

echo -e "${GREEN}Credentials saved to .env.deploy${NC}"
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}  Done! Your app will be live in ~5 minutes${NC}"
echo -e "${GREEN}============================================${NC}"
