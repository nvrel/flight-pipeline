#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/env-cluster.sh"

MODE="${1:-all}"           # ex: all | prepare | join | ...
LAGS="${2:-$FP_LAGS}"
MONTH="${3:-}"             # ex: 201201 ; vide => tous les mois

# --- Résoudre le JAR sur CephFS si non fourni ---
if [[ -z "${FP_APP_JAR}" ]]; then
  FP_APP_JAR="$(ls -1 "$FP_CEPH_BASE"/apps/*assembly*.jar 2>/dev/null | head -n1 || true)"
fi
if [[ -z "${FP_APP_JAR}" || ! -f "${FP_APP_JAR}" ]]; then
  echo "[ERROR] JAR *assembly* introuvable sous $FP_CEPH_BASE/apps. Exporte FP_APP_JAR ou copie le JAR."
  exit 2
fi

# --- Créer les dossiers techniques Spark en HDFS (PATH, sans schéma) ---
hdfs dfs -mkdir -p "$FP_HDFS_ROOT/.sparkStaging" \
               "$FP_HDFS_ROOT/spark-events" \
               "$FP_HDFS_ROOT/spark-warehouse" >/dev/null 2>&1 || true
hdfs dfs -mkdir -p "$FP_P_OUT" >/dev/null 2>&1 || true

# --- Sanity-check des *répertoires* racines (existent sur HDFS) ---
missing=0
for p in "$FP_P_FLIGHTS" "$FP_P_WEATHER" "$FP_P_AIRPORT"; do
  if ! hdfs dfs -test -e "$p"; then
    echo "[ERROR] Chemin HDFS absent: $p"
    missing=1
  fi
done
(( missing )) && exit 3

# --- Filtrage *amont* par mois (limite la lecture au strict nécessaire) -----------
if [[ -n "${MONTH}" ]]; then
  IN_FLIGHTS="${FP_U_FLIGHTS%/}/${MONTH}.csv"
  IN_WEATHER="${FP_U_WEATHER%/}/${MONTH}hourly.txt"
else
  # IMPORTANT : on laisse un répertoire (sans wildcard),
  # le wildcard "*.csv" est ajouté DANS le code Scala.
  IN_FLIGHTS="${FP_U_FLIGHTS%/}"
  IN_WEATHER="${FP_U_WEATHER%/}"
fi
# Vérifie qu'on a bien au moins 1 fichier qui matche
for u in "$IN_FLIGHTS" "$IN_WEATHER"; do
  if ! hdfs dfs -ls "$u" >/dev/null 2>&1; then
    echo "[ERROR] Aucun fichier ne correspond à : $u"
    exit 4
  fi
done

# --- Infos ---
echo "[INFO] NN: $(hdfs getconf -confKey fs.defaultFS)  (FP_HDFS_NN=$FP_HDFS_NN)"
echo "[INFO] JAR: $FP_APP_JAR"
echo "[INFO] In  (URI): flights=$IN_FLIGHTS"
echo "[INFO]              weather=$IN_WEATHER"
echo "[INFO]              airport=$FP_U_AIRPORT"
echo "[INFO] Out (URI):  $FP_U_OUT"
[[ -n "$MONTH" ]] && echo "[INFO] Sample month: $MONTH"

APP_NAME="${FP_APP_NAME_PREFIX} ${MODE} lags=${LAGS}${MONTH:+ month=${MONTH}}"

# --- Submit --------------------------------------------------------------------
spark-submit \
  --name "${APP_NAME}" \
  --class "${FP_MAIN_CLASS}" \
  --master yarn \
  --deploy-mode cluster \
  --packages "${FP_DELTA_COORD}" \
  --queue "${FP_YARN_QUEUE}" \
  --driver-memory "${FP_DRIVER_MEM}" \
  --executor-memory "${FP_EXEC_MEM}" \
  --executor-cores "${FP_EXEC_CORES}" \
  --num-executors "${FP_NUM_EXECUTORS}" \
  --conf "spark.executor.memoryOverhead=${FP_EXEC_OVERHEAD}" \
  --conf "spark.yarn.jars=${FP_SPARK_YARN_JARS}" \
  --conf "spark.sql.extensions=${FP_DELTA_EXT}" \
  --conf "spark.sql.catalog.spark_catalog=${FP_DELTA_CAT}" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.sql.adaptive.skewJoin.enabled=true" \
  --conf "spark.serializer=${FP_SERIALIZER}" \
  --conf "spark.kryoserializer.buffer.max=${FP_KRYO_MAXBUF}" \
  --conf "spark.default.parallelism=${FP_DEFAULT_PAR}" \
  --conf "spark.sql.shuffle.partitions=${FP_SHUFFLE_PARTS}" \
  --conf "spark.eventLog.enabled=true" \
  --conf "spark.eventLog.dir=${FP_U_EVENTS}" \
  --conf "spark.sql.warehouse.dir=${FP_U_WAREHOUSE}" \
  --conf "spark.yarn.stagingDir=${FP_U_STAGING}" \
  --conf "spark.driver.extraJavaOptions=-Dapp.flights-dir=${IN_FLIGHTS} \
                                         -Dapp.weather-dir=${IN_WEATHER} \
                                         -Dapp.airport-csv=${FP_U_AIRPORT} \
                                         -Dapp.out-root=${FP_U_OUT}" \
  --conf "spark.executor.extraJavaOptions=-Dapp.flights-dir=${IN_FLIGHTS} \
                                          -Dapp.weather-dir=${IN_WEATHER} \
                                          -Dapp.airport-csv=${FP_U_AIRPORT} \
                                          -Dapp.out-root=${FP_U_OUT}" \
  "${FP_APP_JAR}" \
  --mode="${MODE}" \
  --flights="${IN_FLIGHTS}" \
  --weather="${IN_WEATHER}" \
  --airport="${FP_U_AIRPORT}" \
  --out="${FP_U_OUT}" \
  --hours=${FP_HOURS} \
  --lags="${LAGS}" \
  ${MONTH:+--sample-month=${MONTH}} \
  2>&1 | tee "run_${MODE}_lags${LAGS}_cluster${MONTH:+_${MONTH}}.log"
