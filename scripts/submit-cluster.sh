#!/usr/bin/env bash
set -euo pipefail

# scripts/submit-cluster.sh
# Variables overrides possibles :
#   FP_EXEC_MEM=8g FP_DRIVER_MEM=4g FP_EXEC_CORES=4 FP_NUM_EXECUTORS=6 bash scripts/submit-cluster.sh join 0

MODE="${1:-all}"
LAGS="${2:-7}"

sbt -no-colors clean assembly
JAR=$(ls target/scala-2.12/*assembly*.jar | head -n1)

: "${FP_EXEC_MEM:=8g}"
: "${FP_DRIVER_MEM:=4g}"
: "${FP_EXEC_CORES:=4}"
: "${FP_NUM_EXECUTORS:=6}"
: "${FP_SHUFFLE_PARTS:=200}"
: "${FP_DEFAULT_PAR:=200}"
: "${FP_BROADCAST:=64m}"
: "${FP_DELTA_COORD:=io.delta:delta-spark_2.12:3.2.0}"
: "${FP_DELTA_EXT:=io.delta.sql.DeltaSparkSessionExtension}"
: "${FP_DELTA_CAT:=org.apache.spark.sql.delta.catalog.DeltaCatalog}"

spark-submit \
  --class flightpipeline.Main \
  --master yarn \
  --deploy-mode cluster \
  --packages "${FP_DELTA_COORD}" \
  --driver-memory "${FP_DRIVER_MEM}" \
  --executor-memory "${FP_EXEC_MEM}" \
  --executor-cores "${FP_EXEC_CORES}" \
  --num-executors "${FP_NUM_EXECUTORS}" \
  --conf "spark.sql.extensions=${FP_DELTA_EXT}" \
  --conf "spark.sql.catalog.spark_catalog=${FP_DELTA_CAT}" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.sql.adaptive.skewJoin.enabled=true" \
  --conf "spark.sql.shuffle.partitions=${FP_SHUFFLE_PARTS}" \
  --conf "spark.default.parallelism=${FP_DEFAULT_PAR}" \
  --conf "spark.sql.autoBroadcastJoinThreshold=${FP_BROADCAST}" \
  "$JAR" \
  --mode="${MODE}" \
  --flights="hdfs:///projects/flight-pipeline/data/Flights" \
  --weather="hdfs:///projects/flight-pipeline/data/Weather" \
  --airport="hdfs:///projects/flight-pipeline/data/wban_airport_timezone.csv" \
  --out="hdfs:///projects/flight-pipeline/out" \
  --hours=12 \
  --lags="${LAGS}"
