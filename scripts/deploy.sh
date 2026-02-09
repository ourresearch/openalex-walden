#!/bin/bash
set -e

echo "WARNING: Local deploys are discouraged"
echo ""
echo "The recommended way to deploy is:"
echo "  1. Commit your changes"
echo "  2. Push to main"
echo "  3. GitHub Actions will deploy automatically"
echo ""
echo "Local deploys can accidentally delete jobs if you haven't pulled latest."
echo "Only use this for emergencies."
echo ""
read -p "Are you sure you want to deploy locally? (y/N) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
fi

echo ""
echo "Pulling latest changes first..."
git pull origin main

echo ""
echo "Validating..."
databricks bundle validate

echo ""
echo "Deploying..."
databricks bundle deploy

echo ""
echo "Deploy complete"
