#!/usr/bin/env bash
# scripts/env-local.sh
# Paramètres "sweet spot" pour ta machine WSL (24 cœurs, SSD, ~5 Go de data)
# Tu peux ajuster seulement ces variables ; les scripts les relisent.

# CPU
export FP_LOCAL_CORES="${FP_LOCAL_CORES:-20}"

# Mémoire
export FP_DRIVER_MEM="${FP_DRIVER_MEM:-12g}"
export FP_EXEC_MEM="${FP_EXEC_MEM:-12g}"            # en local = driver

# Shuffle & parallélisme
export FP_SHUFFLE_PARTS="${FP_SHUFFLE_PARTS:-240}"  # ≈ cores * 12
export FP_DEFAULT_PAR="${FP_DEFAULT_PAR:-240}"

# Lecture fichiers (split)
export FP_MAX_SPLIT="${FP_MAX_SPLIT:-64m}"          # 32m si débit disque faible

# Broadcast (météo join)
export FP_BROADCAST="${FP_BROADCAST:-64m}"

# Répertoire temp (spill / shuffle)
export FP_LOCAL_DIR="${FP_LOCAL_DIR:-/tmp/spark-tmp}"

# GC & sérialisation
export FP_DRIVER_JAVA_OPTS="${FP_DRIVER_JAVA_OPTS:--XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:+ParallelRefProcEnabled}"
export FP_EXEC_JAVA_OPTS="${FP_EXEC_JAVA_OPTS:--XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:+ParallelRefProcEnabled}"
export FP_SERIALIZER="org.apache.spark.serializer.KryoSerializer"
export FP_KRYO_MAXBUF="${FP_KRYO_MAXBUF:-256m}"

# Delta Lake
export FP_DELTA_COORD="io.delta:delta-spark_2.12:3.2.0"
export FP_DELTA_EXT="io.delta.sql.DeltaSparkSessionExtension"
export FP_DELTA_CAT="org.apache.spark.sql.delta.catalog.DeltaCatalog"