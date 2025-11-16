#!/usr/bin/env bash
set -euo pipefail

# scripts/submit-local.sh
# Usage "classique" (pipeline Spark) :
#   bash scripts/submit-local.sh                      # mode=all, lags=7
#   bash scripts/submit-local.sh join 0               # mode=join, lags=0
#   FP_LOCAL_CORES=8 bash scripts/submit-local.sh     # override ponctuel
#
# Mode de restitution :
#   bash scripts/submit-local.sh show-runs            # affiche les derniers runs
#   bash scripts/submit-local.sh show-runs 100        # affiche les 100 derniers runs

# 1) Paramètres locaux centralisés
source "$(dirname "$0")/env-local.sh"

# Valeurs par défaut si l'environnement ne les définit pas déjà
: "${FP_OUT_ROOT:=out}"
: "${FP_DELAY_DATASET:=D2}"         # D3 = bad-weather delays (article, section 4.2)
: "${FP_FEATURE_SET:=with-weather}" # with-weather / no-weather
: "${FP_OUT_ROOT:=out}"

CMD="${1:-}"   # "all", "prepare", "join", "training", "train", "quality", "report", "show-runs"

# ---------------------------------------------------------------------------
# 2) Mode spécial : restitution des runs d'entraînement
# ---------------------------------------------------------------------------
if [[ "$CMD" == "show-runs" ]]; then
  shift || true
  SHOW_LIMIT="${1:-50}"

  export FP_OUT_ROOT
  export FP_SHOW_LIMIT="$SHOW_LIMIT"

  "$SPARK_HOME/bin/spark-shell" \
    --master "local[${FP_LOCAL_CORES}]" \
    --driver-memory "${FP_DRIVER_MEM}" \
    --packages "${FP_DELTA_COORD}" \
    --conf "spark.sql.extensions=${FP_DELTA_EXT}" \
    --conf "spark.sql.catalog.spark_catalog=${FP_DELTA_CAT}" <<'EOF'

import org.apache.spark.sql.functions._

val outRoot   = sys.env.getOrElse("FP_OUT_ROOT", "out")
val limit     = sys.env.getOrElse("FP_SHOW_LIMIT", "50").toInt
val path      = s"$outRoot/metrics/train_runs"

val runs = spark.read.format("delta").load(path)

val display = runs
  .orderBy(col("ts").desc)
  .select(
    col("ts"),
    col("location"),
    col("dataset_id"),
    col("delay_threshold_min"),
    col("lags"),
    col("window_hours"),
    col("n_train"),
    col("n_test"),
    col("test_accuracy"),
    col("test_recall_pos").alias("Recd"),
    col("test_specificity").alias("Reco"),
    col("comment")
  )

display.show(limit, truncate = false)

sys.exit(0)
EOF

  exit 0
fi

# ---------------------------------------------------------------------------
# 3) Mode "normal" : exécution du pipeline Spark
# ---------------------------------------------------------------------------

MODE="${CMD:-all}"                           # all / prepare / join / training / ...
LAGS="${2:-7}"                               # nombre de lags
DELAY_DATASET="${3:-$FP_DELAY_DATASET}"      # D1 / D2 / D3 / D4 / ALL
FEATURE_SET="${4:-$FP_FEATURE_SET}"          # with-weather / no-weather
MONTH="${5:-}"                               # ex: 201201

# Normalisation : "train" → "training"
case "$MODE" in
  train)
    MODE="training"
    ;;
esac

# 3.1) Build
sbt -no-colors clean assembly
JAR=$(ls target/scala-2.12/*assembly*.jar | head -n1)

mkdir -p "$FP_LOCAL_DIR" logs

# 3.2) Informations Git pour le logger d'entraînement
GIT_COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")"
if git diff --quiet --ignore-submodules HEAD 2>/dev/null; then
  GIT_DIRTY="false"
else
  GIT_DIRTY="true"
fi

export FP_ENV_LOCATION="local"
export FP_GIT_COMMIT="$GIT_COMMIT"
export FP_GIT_DIRTY="$GIT_DIRTY"
export FP_OUT_ROOT

# 3.3) Construction de la liste d’arguments Scala
CLI_ARGS=(
  --mode "$MODE"
  --flights "data/Flights"
  --weather "data/Weather"
  --airport "data/wban_airport_timezone.csv"
  --out "$FP_OUT_ROOT"
  --hours 12
  --delay-threshold-min 60
  --lags "$LAGS"
  --delay-dataset "$DELAY_DATASET"
  --feature-set "$FEATURE_SET"
)


if [[ -n "$MONTH" ]]; then
  CLI_ARGS+=(--sample-month "$MONTH")
fi

# 3.4) Lancement du pipeline
spark-submit \
  --class flightpipeline.Main \
  --master "local[${FP_LOCAL_CORES}]" \
  --conf "spark.ui.port=${FP_UI_PORT:-4040}" \
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
  "${CLI_ARGS[@]}" \
  2>&1 | tee "logs/run_${MODE}_lags${LAGS}_local${MONTH:+_${MONTH}}.log"
