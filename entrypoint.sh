#!/bin/sh
set -e

echo "Running migrations..."
/app/migrate-apply up

echo "Starting application..."
exec /app/grogu
