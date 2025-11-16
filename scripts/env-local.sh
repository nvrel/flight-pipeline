#!/usr/bin/env bash
# scripts/env-local.sh
# Paramètres d’exécution locale sous WSL pour flight-pipeline.
# L’idée est de garder un parallélisme raisonnable et un heap généreux
# afin de supporter l’entraînement Random Forest sur plusieurs centaines
# de milliers de lignes.

# --------------------------------------------------------------------
# CPU / Mémoire
# --------------------------------------------------------------------

# Nombre de cœurs utilisés en local : --master local[FP_LOCAL_CORES]
# Avec WSL limité à ~32 Gio de RAM, 8 cœurs reste un bon compromis.
export FP_LOCAL_CORES="${FP_LOCAL_CORES:-8}"

# Mémoire JVM côté driver / exécuteur (en local, driver = exécuteur).
# Ces valeurs supposent que WSL dispose d’au moins ~32 Gio.
# Si WSL est configuré avec davantage de mémoire, ces valeurs
# peuvent être augmentées (par exemple 28g ou 32g).
export FP_DRIVER_MEM="${FP_DRIVER_MEM:-24g}"
export FP_EXEC_MEM="${FP_EXEC_MEM:-24g}"

# Répertoire temporaire Spark (spill, shuffle, broadcast).
# Doit pointer sur un disque rapide avec suffisamment d’espace.
export FP_LOCAL_DIR="${FP_LOCAL_DIR:-/tmp/spark-tmp}"

# --------------------------------------------------------------------
# Shuffle / parallélisme
# --------------------------------------------------------------------

# Nombre de partitions pour les gros shuffle (joins, groupBy, RF…).
# À garder dans le même ordre de grandeur que FP_LOCAL_CORES mais
# avec un facteur supplémentaire pour éviter des partitions trop grosses.
export FP_SHUFFLE_PARTS="${FP_SHUFFLE_PARTS:-96}"

# Parallélisme par défaut de Spark (RDD/Dataset sans partitionnement explicite).
export FP_DEFAULT_PAR="${FP_DEFAULT_PAR:-96}"

# Taille maximale d’un bloc lu sur disque (parquet / CSV).
# Valeur plus petite = plus de partitions d’entrée, donc plus de tâches mais
# des blocs mémoire plus modestes.
export FP_MAX_SPLIT="${FP_MAX_SPLIT:-64m}"

# Seuil de diffusion automatique des petites tables en broadcast.
export FP_BROADCAST="${FP_BROADCAST:-64m}"

# --------------------------------------------------------------------
# JVM / sérialisation
# --------------------------------------------------------------------

# Options JVM appliquées au driver.
export FP_DRIVER_JAVA_OPTS="${FP_DRIVER_JAVA_OPTS:--XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:+ParallelRefProcEnabled}"

# Options JVM appliquées à l’exécuteur (en local, identiques au driver).
export FP_EXEC_JAVA_OPTS="${FP_EXEC_JAVA_OPTS:--XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:+ParallelRefProcEnabled}"

# Sérialisation recommandée pour les jobs ML sur gros volumes.
export FP_SERIALIZER="${FP_SERIALIZER:-org.apache.spark.serializer.KryoSerializer}"
export FP_KRYO_MAXBUF="${FP_KRYO_MAXBUF:-256m}"

# --------------------------------------------------------------------
# Delta Lake
# --------------------------------------------------------------------

export FP_DELTA_COORD="${FP_DELTA_COORD:-io.delta:delta-spark_2.12:3.2.0}"
export FP_DELTA_EXT="${FP_DELTA_EXT:-io.delta.sql.DeltaSparkSessionExtension}"
export FP_DELTA_CAT="${FP_DELTA_CAT:-org.apache.spark.sql.delta.catalog.DeltaCatalog}"

# --------------------------------------------------------------------
# UI Spark (utile sous WSL)
# --------------------------------------------------------------------

# Force Spark à écouter sur l’interface loopback.
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_PUBLIC_DNS=localhost

# Port de l’UI Spark (Spark auto-incrémente si le port est déjà occupé).
export FP_UI_PORT="${FP_UI_PORT:-4040}"

# --------------------------------------------------------------------
# Hyperparamètres contrôlables par l’environnement
# --------------------------------------------------------------------

# Taille maximale par classe (positif / négatif) pour l’échantillon équilibré
# utilisé par l’entraînement du Random Forest.
# La valeur par défaut vise un dataset de l’ordre du million de lignes
# (train + test). Sur le cluster, cette limite pourra être augmentée.
#export FP_RF_MAX_ROWS_PER_CLASS="${FP_RF_MAX_ROWS_PER_CLASS:-400000}"
export FP_RF_MAX_ROWS_PER_CLASS=1000000
# --------------------------------------------------------------------
# Métadonnées des runs d'entraînement (journalisation)
# --------------------------------------------------------------------

# Tag de localisation logique pour les runs : "local" ou "cluster" par exemple.
# TrainRunLogger l’enregistre dans la colonne `location` pour distinguer
# facilement ce qui a été fait en WSL, sur le cluster, etc.
export FP_TRAIN_LOCATION="${FP_TRAIN_LOCATION:-local}"

# Commentaire libre associé au run (description de l’essai, ex:
# "baseline 7 lags, mois 2008-01"). Si vide, la colonne `comment`
# sera simplement nulle dans le journal.
export FP_RUN_COMMENT="${FP_RUN_COMMENT:-}"