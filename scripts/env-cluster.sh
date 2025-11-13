#!/usr/bin/env bash
# ===================  ENV CLUSTER (HDFS + CephFS + Spark)  ===================
# Basé sur ta version ; ajouts: FP_SPARK_YARN_JARS et nom d'app. par défaut.

# --- CephFS (POSIX) ---
export FP_CEPH_BASE="${FP_CEPH_BASE:-/opt/cephfs/users/students/p6emiasd2025/nvrel}"

# --- Racines HDFS relatives à TON espace ---
export FP_HDFS_ROOT="${FP_HDFS_ROOT:-/students/p6emiasd2025/nvrel}"
export FP_HDFS_BASE="${FP_HDFS_BASE:-$FP_HDFS_ROOT/flight-pipeline}"

# --- fs.defaultFS => URI du NameNode (authority) ---
FP_NN_DEFAULT="$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || echo 'hdfs://10.1.4.80:9000')"
export FP_HDFS_NN="${FP_HDFS_NN:-$FP_NN_DEFAULT}"
export FP_HDFS_NN="${FP_HDFS_NN%/}"

# --- Chemins HDFS (PATH, sans schéma) ---
export FP_P_FLIGHTS="${FP_P_FLIGHTS:-$FP_HDFS_BASE/data/Flights}"
export FP_P_WEATHER="${FP_P_WEATHER:-$FP_HDFS_BASE/data/Weather}"
export FP_P_AIRPORT="${FP_P_AIRPORT:-$FP_HDFS_BASE/data/wban_airport_timezone.csv}"
export FP_P_OUT="${FP_P_OUT:-$FP_HDFS_BASE/out}"

# --- URI HDFS (avec authority) pour Spark ---
root_trim="${FP_HDFS_ROOT#/}"
base_trim="${FP_HDFS_BASE#/}"
export FP_U_ROOT="${FP_HDFS_NN}/${root_trim}"
export FP_U_BASE="${FP_HDFS_NN}/${base_trim}"
export FP_U_FLIGHTS="${FP_HDFS_NN}/${FP_P_FLIGHTS#/}"
export FP_U_WEATHER="${FP_HDFS_NN}/${FP_P_WEATHER#/}"
export FP_U_AIRPORT="${FP_HDFS_NN}/${FP_P_AIRPORT#/}"
export FP_U_OUT="${FP_HDFS_NN}/${FP_P_OUT#/}"

# --- Répertoires Spark (URI, dans TON HDFS) ---
export FP_U_STAGING="${FP_U_ROOT}/.sparkStaging"
export FP_U_EVENTS="${FP_U_ROOT}/spark-events"
export FP_U_WAREHOUSE="${FP_U_ROOT}/spark-warehouse"

# --- JAR (sur CephFS) ---
export FP_APP_JAR="${FP_APP_JAR:-}"

# --- Ressources YARN (preset issu de ton probe : 11 x 4c, 9g) ---
export FP_NUM_EXECUTORS="${FP_NUM_EXECUTORS:-11}"
export FP_EXEC_CORES="${FP_EXEC_CORES:-4}"
export FP_EXEC_MEM="${FP_EXEC_MEM:-9g}"
export FP_EXEC_OVERHEAD="${FP_EXEC_OVERHEAD:-1024}"  # MB
export FP_DRIVER_MEM="${FP_DRIVER_MEM:-4g}"
export FP_YARN_QUEUE="${FP_YARN_QUEUE:-default}"

# Partitions par défaut (peuvent être overridées avant submit)
FP_TOTAL_CORES=$(( FP_NUM_EXECUTORS * FP_EXEC_CORES ))
export FP_DEFAULT_PAR="${FP_DEFAULT_PAR:-$(( FP_TOTAL_CORES * 4 ))}"
export FP_SHUFFLE_PARTS="${FP_SHUFFLE_PARTS:-$(( FP_TOTAL_CORES * 8 ))}"

# --- Delta & sérialisation ---
export FP_DELTA_COORD="${FP_DELTA_COORD:-io.delta:delta-spark_2.12:3.2.0}"
export FP_DELTA_EXT="${FP_DELTA_EXT:-io.delta.sql.DeltaSparkSessionExtension}"
export FP_DELTA_CAT="${FP_DELTA_CAT:-org.apache.spark.sql.delta.catalog.DeltaCatalog}"
export FP_SERIALIZER="${FP_SERIALIZER:-org.apache.spark.serializer.KryoSerializer}"
export FP_KRYO_MAXBUF="${FP_KRYO_MAXBUF:-256m}"

# --- Nom d'app par défaut ---
export FP_MAIN_CLASS="${FP_MAIN_CLASS:-flightpipeline.Main}"
export FP_APP_NAME_PREFIX="${FP_APP_NAME_PREFIX:-flight-pipeline}"

# --- Hyperparamètres article ---
export FP_HOURS="${FP_HOURS:-12}"
export FP_LAGS="${FP_LAGS:-7}"

# --- Évite l'upload massif des jars Spark à chaque run (présents sur tous les nœuds) ---
# Si /opt/shared/spark-3.5.1-bin-hadoop3/jars/* est bien présent partout :
export FP_SPARK_YARN_JARS="${FP_SPARK_YARN_JARS:-local:/opt/shared/spark-3.5.1-bin-hadoop3/jars/*}"
