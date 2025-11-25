#!/usr/bin/env bash
set -euo pipefail

# scripts/clean-cluster.sh
# Nettoyage :
#   - objets Spark SQL locaux (db flight_project)
#   - répertoires locaux (out/, metastore_db/, etc.)
#   - tables Delta intermédiaires sur HDFS (join_intermediate, join_flat_lag*)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# On essaie de charger env-cluster.sh pour récupérer FP_P_OUT / FP_HDFS_BASE, etc.
if [[ -f "${SCRIPT_DIR}/env-cluster.sh" ]]; then
  # shellcheck disable=SC1090
  source "${SCRIPT_DIR}/env-cluster.sh"
fi

echo "[clean-cluster] Dropping any local SQL objects (ignore errors if none)…"

# Drop tables si la base existe
if spark-sql -S -e "SHOW DATABASES LIKE 'flight_project';" | grep -q "^flight_project$"; then
  echo "[clean-cluster] - Dropping tables from flight_project"
  spark-sql -S -e "DROP TABLE IF EXISTS flight_project.flight_clean"
  spark-sql -S -e "DROP TABLE IF EXISTS flight_project.weather_clean"
  spark-sql -S -e "DROP TABLE IF EXISTS flight_project.airport_timezone_clean"
  spark-sql -S -e "DROP TABLE IF EXISTS flight_project.joined_data"
fi

# Drop database (syntaxe correcte : IF EXISTS)
echo "[clean-cluster] - Dropping database flight_project (if exists)"
spark-sql -S -e "DROP DATABASE IF EXISTS flight_project CASCADE"

echo "[clean-cluster] Removing generated local data and work dirs…"
rm -rf out/ metastore_db/ spark-warehouse/ derby.log target/

# ---------------------------------------------------------------------------
# Nettoyage des tables Delta intermédiaires sur HDFS (cluster)
# ---------------------------------------------------------------------------
# On utilise FP_P_OUT si défini par env-cluster.sh, sinon on n’essaie pas.
if command -v hdfs >/dev/null 2>&1 && [[ -n "${FP_P_OUT:-}" ]]; then
  echo "[clean-cluster] Removing Delta join outputs on HDFS under ${FP_P_OUT} (ignore errors if missing)…"

  # Table join_intermediate.parquet (intermédiaire vols/météo)
  hdfs dfs -rm -r -f "${FP_P_OUT}/join_intermediate.parquet" || true

  # Tables join_flat_lagX.parquet si elles existent (X = 0,1,2,…)
  # On laisse HDFS gérer le glob ; si rien ne matche, on ignore l'erreur.
  hdfs dfs -rm -r -f "${FP_P_OUT}/join_flat_lag"*".parquet" 2>/dev/null || true
fi

echo "[clean-cluster] Done."
