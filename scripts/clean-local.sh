#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "[clean-local] Stopping any local Spark shells… (if any open)"

/usr/bin/env pkill -f 'org.apache.spark.deploy' || true

echo "[clean-local] Dropping any local SQL tables (ignore errors if none)…"
spark-shell --master local[2] \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  <<'SCALA'
spark.conf.set("spark.sql.catalogImplementation","in-memory")
spark.sql("DROP TABLE IF EXISTS flight_project.flight_clean")
spark.sql("DROP TABLE IF EXISTS flight_project.weather_clean")
spark.sql("DROP TABLE IF EXISTS flight_project.airport_timezone_clean")
spark.sql("DROP DATABASE IF NOT EXISTS flight_project CASCADE")
System.exit(0)
SCALA

echo "[clean-local] Removing generated data and work dirs…"
rm -rf out out-sample spark-warehouse metastore metastore_db logs target derby.log || true

echo "[clean-local] Done."
