#!/usr/bin/env bash
# Manual MinIO bucket initialization script
# Run this if minio-init container fails: bash scripts/init_minio.sh

set -e

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_ACCESS_KEY="${MINIO_ROOT_USER:-admin}"
MINIO_SECRET_KEY="${MINIO_ROOT_PASSWORD:-password123}"

echo "Waiting for MinIO to be ready..."
until curl -sf "${MINIO_ENDPOINT}/minio/health/live"; do
  sleep 2
done

echo "Configuring mc client..."
mc alias set myminio "${MINIO_ENDPOINT}" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}"

echo "Creating buckets..."
mc mb --ignore-existing myminio/stg
mc mb --ignore-existing myminio/dds
mc mb --ignore-existing myminio/scripts

echo "Setting public access..."
mc anonymous set public myminio/stg
mc anonymous set public myminio/dds

echo "Bucket listing:"
mc ls myminio

echo "MinIO initialization complete!"
