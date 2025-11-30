#!/usr/bin/env bash
# scripts/env-cluster.sh
# Paramètres d’exécution sur le CLUSTER pour flight-pipeline.
# On garde tes chemins existants et on ajoute juste ce qu’il faut pour Spark.

# ---------------------------------------------------------------------------
# Stockage CephFS (POSIX)
# ---------------------------------------------------------------------------
export FP_CEPH_BASE="${FP_CEPH_BASE:-/opt/cephfs/users/students/p6emiasd2025/nvrel}"

# ---------------------------------------------------------------------------
# Racines HDFS dans TON espace
# ---------------------------------------------------------------------------
export FP_HDFS_ROOT="${FP_HDFS_ROOT:-/students/p6emiasd2025/nvrel}"
export FP_HDFS_BASE="${FP_HDFS_BASE:-$FP_HDFS_ROOT/flight-pipeline}"

# URI du NameNode (fs.defaultFS)
FP_NN_DEFAULT="$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || echo 'hdfs://10.1.4.80:9000')"
export FP_HDFS_NN="${FP_HDFS_NN:-$FP_NN_DEFAULT}"
# enlève un éventuel / final
export FP_HDFS_NN="${FP_HDFS_NN%/}"

# ---------------------------------------------------------------------------
# Chemins HDFS (SANS schéma) — utilisés par hdfs dfs -ls, -mkdir, etc.
# ---------------------------------------------------------------------------
export FP_P_FLIGHTS="${FP_P_FLIGHTS:-$FP_HDFS_BASE/data/Flights}"
export FP_P_WEATHER="${FP_P_WEATHER:-$FP_HDFS_BASE/data/Weather}"
export FP_P_AIRPORT="${FP_P_AIRPORT:-$FP_HDFS_BASE/data/wban_airport_timezone.csv}"
export FP_P_OUT="${FP_P_OUT:-$FP_HDFS_BASE/out}"

# ---------------------------------------------------------------------------
# URI HDFS (AVEC schéma + namenode) pour Spark
# ---------------------------------------------------------------------------
export FP_U_ROOT="${FP_HDFS_NN}/${FP_HDFS_ROOT#/}"
export FP_U_BASE="${FP_HDFS_NN}/${FP_HDFS_BASE#/}"
export FP_U_FLIGHTS="${FP_HDFS_NN}/${FP_P_FLIGHTS#/}"
export FP_U_WEATHER="${FP_HDFS_NN}/${FP_P_WEATHER#/}"
export FP_U_AIRPORT="${FP_HDFS_NN}/${FP_P_AIRPORT#/}"
export FP_U_OUT="${FP_HDFS_NN}/${FP_P_OUT#/}"

# Répertoires Spark sur HDFS (staging, events, warehouse)
export FP_U_STAGING="${FP_U_ROOT}/.sparkStaging"
export FP_U_EVENTS="${FP_U_ROOT}/spark-events"
export FP_U_WAREHOUSE="${FP_U_ROOT}/spark-warehouse"

# ---------------------------------------------------------------------------
# Ressources YARN (adaptées à ton cluster, valeurs conservées)
# ---------------------------------------------------------------------------
#export FP_NUM_EXECUTORS="${FP_NUM_EXECUTORS:-11}"
export FP_NUM_EXECUTORS="${FP_NUM_EXECUTORS:-13}"
export FP_EXEC_CORES="${FP_EXEC_CORES:-4}"
export FP_EXEC_MEM="${FP_EXEC_MEM:-9g}"
export FP_EXEC_OVERHEAD="${FP_EXEC_OVERHEAD:-1024}"   # en MB
export FP_DRIVER_MEM="${FP_DRIVER_MEM:-4g}"
export FP_YARN_QUEUE="${FP_YARN_QUEUE:-default}"

FP_TOTAL_CORES=$(( FP_NUM_EXECUTORS * FP_EXEC_CORES ))
export FP_DEFAULT_PAR="${FP_DEFAULT_PAR:-$(( FP_TOTAL_CORES * 4 ))}"
export FP_SHUFFLE_PARTS="${FP_SHUFFLE_PARTS:-$(( FP_TOTAL_CORES * 8 ))}"

# Taille des partitions / broadcast
export FP_MAX_SPLIT="${FP_MAX_SPLIT:-64m}"
export FP_BROADCAST="${FP_BROADCAST:-64m}"

# ---------------------------------------------------------------------------
# Delta / sérialisation / JVM
# ---------------------------------------------------------------------------
export FP_DELTA_COORD="${FP_DELTA_COORD:-io.delta:delta-spark_2.12:3.2.0}"
export FP_DELTA_EXT="${FP_DELTA_EXT:-io.delta.sql.DeltaSparkSessionExtension}"
export FP_DELTA_CAT="${FP_DELTA_CAT:-org.apache.spark.sql.delta.catalog.DeltaCatalog}"

export FP_SERIALIZER="${FP_SERIALIZER:-org.apache.spark.serializer.KryoSerializer}"
export FP_KRYO_MAXBUF="${FP_KRYO_MAXBUF:-256m}"

export FP_DRIVER_JAVA_OPTS="${FP_DRIVER_JAVA_OPTS:--XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:+ParallelRefProcEnabled}"
export FP_EXEC_JAVA_OPTS="${FP_EXEC_JAVA_OPTS:--XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:+ParallelRefProcEnabled}"

# ---------------------------------------------------------------------------
# Spark cluster
# ---------------------------------------------------------------------------
export FP_CLUSTER_MASTER="${FP_CLUSTER_MASTER:-yarn}"
export FP_CLUSTER_DEPLOY_MODE="${FP_CLUSTER_DEPLOY_MODE:-cluster}"

# Répertoire pour fichiers temporaires / logs côté CephFS
export FP_CLUSTER_DIR="${FP_CLUSTER_DIR:-$FP_CEPH_BASE/spark-local}"

# Évite de ré-uploader les jars Spark à chaque run
export FP_SPARK_YARN_JARS="${FP_SPARK_YARN_JARS:-local:/opt/shared/spark-3.5.1-bin-hadoop3/jars/*}"

# ---------------------------------------------------------------------------
# Paramètres métier (alignés sur env-local)
# ---------------------------------------------------------------------------
export FP_HOURS="${FP_HOURS:-12}"
export FP_LAGS="${FP_LAGS:-7}"

# Dataset / features par défaut
export FP_DELAY_DATASET="${FP_DELAY_DATASET:-D2}"
export FP_FEATURE_SET="${FP_FEATURE_SET:-with-weather}"

# Journalisation des runs d'entraînement
export FP_TRAIN_LOCATION="${FP_TRAIN_LOCATION:-cluster}"
export FP_RUN_COMMENT="${FP_RUN_COMMENT:-}"
export FP_RF_MAX_ROWS_PER_CLASS="${FP_RF_MAX_ROWS_PER_CLASS:-2000000}"
