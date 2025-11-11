#!/usr/bin/env bash
set -euo pipefail

echo "[clean-local] Dropping any local SQL objects (ignore errors if none)…"

# Drop tables si la base existe
spark-sql -S -e "SHOW DATABASES LIKE 'flight_project';" | grep -q "^flight_project$" && {
  echo "[clean-local] - Dropping tables from flight_project"
  spark-sql -S -e "DROP TABLE IF EXISTS flight_project.flight_clean"
  spark-sql -S -e "DROP TABLE IF EXISTS flight_project.weather_clean"
  spark-sql -S -e "DROP TABLE IF EXISTS flight_project.airport_timezone_clean"
  spark-sql -S -e "DROP TABLE IF EXISTS flight_project.joined_data"
}

# Drop database (syntaxe correcte : IF EXISTS)
echo "[clean-local] - Dropping database flight_project (if exists)"
spark-sql -S -e "DROP DATABASE IF EXISTS flight_project CASCADE"

echo "[clean-local] Removing generated data and work dirs…"
rm -rf out/ metastore_db/ spark-warehouse/ derby.log target/

echo "[clean-local] Done."
