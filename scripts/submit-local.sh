#!/usr/bin/env bash
set -euo pipefail

# scripts/submit-local.sh
# Usage (exemples) :
#   bash scripts/submit-local.sh                     # mode=all, lags=3 (defaults application.conf)
#   bash scripts/submit-local.sh join 0              # mode=join, lags=0
#   FP_LOCAL_CORES=8 bash scripts/submit-local.sh    # override ponctuel

# 1) Paramètres locaux centralisés
source "$(dirname "$0")/env-local.sh"

MODE="${1:-all}"
LAGS="${2:-7}"
MONTH="${3:-}"        # ex: 201201

# 2) Build
sbt -no-colors clean assembly
JAR=$(ls target/scala-2.12/*assembly*.jar | head -n1)

mkdir -p "$FP_LOCAL_DIR" logs

# 3) Run
spark-submit \
  --class flightpipeline.Main \
  --master "local[${FP_LOCAL_CORES}]" \
  --packages "${FP_DELTA_COORD}" \
  --driver-memory "${FP_DRIVER_MEM}" \
  --conf "spark.executor.memory=${FP_EXEC_MEM}" \
  --conf "spark.local.dir=${FP_LOCAL_DIR}" \
  --conf "spark.sql.extensions=${FP_DELTA_EXT}" \
  --conf "spark.sql.catalog.spark_catalog=${FP_DELTA_CAT}" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
  --conf "spark.sql.adaptive.skewJoin.enabled=true" \
  --conf "spark.sql.shuffle.partitions=${FP_SHUFFLE_PARTS}" \
  --conf "spark.default.parallelism=${FP_DEFAULT_PAR}" \
  --conf "spark.sql.files.maxPartitionBytes=${FP_MAX_SPLIT}" \
  --conf "spark.sql.autoBroadcastJoinThreshold=${FP_BROADCAST}" \
  --conf "spark.serializer=${FP_SERIALIZER}" \
  --conf "spark.kryoserializer.buffer.max=${FP_KRYO_MAXBUF}" \
  --driver-java-options "${FP_DRIVER_JAVA_OPTS}" \
  --conf "spark.executor.extraJavaOptions=${FP_EXEC_JAVA_OPTS}" \
  "$JAR" \
  --mode="${MODE}" \
  --flights="data/Flights" \
  --weather="data/Weather" \
  --airport="data/wban_airport_timezone.csv" \
  --out="out" \
  --hours=12 \
  --lags="${LAGS}" \
  ${MONTH:+--sample-month=${MONTH}} \
  2>&1 | tee "logs/run_${MODE}_lags${LAGS}_local${MONTH:+_${MONTH}}.log"