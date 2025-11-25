#!/usr/bin/env bash
set -euo pipefail

# scripts/submit-cluster.sh
#
# Usage (comme en local) :
#   bash scripts/submit-cluster.sh                      # mode=all, lags=7
#   bash scripts/submit-cluster.sh prepare 7            # PREPARE, lags=7, TOUS les mois
#   bash scripts/submit-cluster.sh join 0               # JOIN, lags=0, TOUS les mois
#   bash scripts/submit-cluster.sh training 7 D2 with-weather
#   bash scripts/submit-cluster.sh training 7 D2 with-weather 201201  # mois = 201201 uniquement
#
# Attention : ce script n’essaie PAS de builder le JAR.
# Il s’attend à trouver un JAR assemblé déjà présent sur le cluster.
# Par défaut : ~/workspace/apps/flight-pipeline-assembly-0.1.0.jar
# (exactement comme run_cluster_sample.sh)

# ---------------------------------------------------------------------------
# 1) Localisation + chargement de la config cluster
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"  # plus utilisé pour sbt

# Charge env-cluster (chemins HDFS/CephFS, ressources Yarn, etc.)
source "${SCRIPT_DIR}/env-cluster.sh"

# Jeux de retards / features par défaut (comme en local)
: "${FP_DELAY_DATASET:=D2}"
: "${FP_FEATURE_SET:=with-weather}"
: "${FP_HOURS:=12}"
: "${FP_LAGS:=7}"

CMD="${1:-}"   # all, prepare, join, training, train, ...

# ---------------------------------------------------------------------------
# 2) Parsing des arguments (symétrique à submit-local.sh)
# ---------------------------------------------------------------------------
MODE="${CMD:-all}"
LAGS="${2:-$FP_LAGS}"
DELAY_DATASET="${3:-$FP_DELAY_DATASET}"
FEATURE_SET="${4:-$FP_FEATURE_SET}"
MONTH="${5:-}"   # optionnel ; s’il est vide, on NE PASSE PAS --sample-month

# train ⇒ training (pour rester compatible avec le local)
case "$MODE" in
  train) MODE="training" ;;
esac

# ---------------------------------------------------------------------------
# 3) Localisation du JAR précompilé (comme run_cluster_sample.sh)
# ---------------------------------------------------------------------------
# Tu peux surcharger FP_APP_JAR dans ton env si tu changes le chemin un jour.
APP_JAR_DEFAULT="${FP_APP_JAR:-$HOME/workspace/apps/flight-pipeline-assembly-0.1.0.jar}"

# expand ~ si besoin
expand_path() { eval "printf %s \"$1\""; }
APP_JAR_PATH="$(expand_path "$APP_JAR_DEFAULT")"
APP_JAR_ABS="$(readlink -f "$APP_JAR_PATH" 2>/dev/null || echo "$APP_JAR_PATH")"

if [[ ! -f "$APP_JAR_ABS" ]]; then
  echo "ERREUR: JAR introuvable : $APP_JAR_ABS" >&2
  echo "  → Build en local avec 'sbt assembly' puis copie sur le cluster (comme pour run_cluster_sample.sh)." >&2
  exit 1
fi

mkdir -p "${FP_CLUSTER_DIR:-$HOME/workspace/logs}" logs

# ---------------------------------------------------------------------------
# 4) Construction de la ligne de commande Scala (identique au local)
# ---------------------------------------------------------------------------
CLI_ARGS=(
  --mode "$MODE"
  --flights "$FP_U_FLIGHTS"
  --weather "$FP_U_WEATHER"
  --airport "$FP_U_AIRPORT"
  --out "$FP_U_OUT"
  --hours "$FP_HOURS"
  --delay-threshold-min 60
  --lags "$LAGS"
  --delay-dataset "$DELAY_DATASET"
  --feature-set "$FEATURE_SET"
)

# IMPORTANT :
# - si MONTH est vide → on NE passe PAS --sample-month → tous les mois
if [[ -n "$MONTH" ]]; then
  CLI_ARGS+=(--sample-month "$MONTH")
fi

log_file="logs/run_${MODE}_lags${LAGS}_cluster${MONTH:+_${MONTH}}.log"

# ---------------------------------------------------------------------------
# 5) Lancement du job Spark sur YARN
# ---------------------------------------------------------------------------
spark-submit \
  --class "${FP_MAIN_CLASS:-flightpipeline.Main}" \
  --master "${FP_CLUSTER_MASTER:-yarn}" \
  --deploy-mode "${FP_CLUSTER_DEPLOY_MODE:-cluster}" \
  --name "${FP_APP_NAME_PREFIX:-flight-pipeline}-${MODE}-lags${LAGS}" \
  --queue "${FP_YARN_QUEUE:-default}" \
  --driver-memory "${FP_DRIVER_MEM:-4g}" \
  --executor-memory "${FP_EXEC_MEM:-9g}" \
  --executor-cores "${FP_EXEC_CORES:-4}" \
  --num-executors "${FP_NUM_EXECUTORS:-11}" \
  --conf "spark.yarn.jars=${FP_SPARK_YARN_JARS}" \
  --conf "spark.yarn.stagingDir=${FP_U_STAGING}" \
  --conf "spark.sql.warehouse.dir=${FP_U_WAREHOUSE}" \
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
  --conf "spark.executor.memoryOverhead=${FP_EXEC_OVERHEAD}" \
  --conf "spark.executor.extraJavaOptions=${FP_EXEC_JAVA_OPTS}" \
  --driver-java-options "${FP_DRIVER_JAVA_OPTS}" \
  --packages "${FP_DELTA_COORD}" \
  "$APP_JAR_ABS" \
  "${CLI_ARGS[@]}" \
  2>&1 | tee "$log_file"
