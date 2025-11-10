# 🛫 Flight-Pipeline

_Pipeline Spark (Scala) pour la préparation et la jointure temporelle des données vols & météo_

---

## 📘 Objectif

Ce projet met en œuvre un pipeline **Apache Spark / Scala** capable de :
- lire et nettoyer les données de vols (Flights) et de météo (Weather),
- enrichir les vols avec les observations météo correspondantes (origine et destination),
- produire des tables Delta (flight_clean, weather_clean, airport_timezone_clean),
- générer des jeux joints exploitables pour la modélisation (ex. prédiction de retards).

Le pipeline fonctionne :
- **en local (WSL/Linux)** pour développement et tests,
- **sur le cluster Hadoop/YARN du LAMSADE** pour exécution distribuée.

---

## ⚙️ Environnement requis

### Local (WSL/Linux)
- **Java 11**
- **Scala 2.12.x**
- **sbt ≥ 1.10.7**
- **Spark 3.5.x**
- Accès réseau au dépôt Maven central
- (Optionnel) Delta Lake 3.2.0 pour Spark local

### Cluster LAMSADE
- Accès SSH via bastion ssh.lamsade.dauphine.fr
- Espace HDFS autorisé : /students/p6emiasd2025/nvrel
- Spark 3.5.1 et YARN disponibles sur vmhadoopmaster.srv.lamsade.dauphine.fr

---

## 🏗️ Structure du projet
```
flight-pipeline/
├── build.sbt
├── project/
├── src/
│   ├── main/scala/flightpipeline/
│   │   ├── Main.scala
│   │   ├── stage/...
│   │   └── util/...
│   └── ...
├── scripts/
│   ├── submit-local.sh
│   ├── submit-cluster.sh
│   └── env-local.sh
├── run_cluster_sample.sh     ← script d'exécution cluster
├── data/                      ← données locales (non versionnées)
├── out/                       ← sorties locales (non versionnées)
└── README.md
```

---

## 🧰 Installation locale et build du JAR

Depuis la racine du projet :
```bash
sbt clean assembly
```

Le JAR exécutable est généré ici : `target/scala-2.12/flight-pipeline-assembly-0.1.0.jar`

---

## 🧪 Exécution locale (WSL)
```bash
scripts/submit-local.sh prepare
scripts/submit-local.sh join 0
```

Les sorties apparaissent dans `out/` :
```
out/flight_clean.parquet
out/weather_clean.parquet
out/join_intermediate.parquet
```

---

## ☁️ Déploiement sur le cluster LAMSADE

### 1️⃣ Accès SSH (bastion + edge node)

Ajouter ceci à `~/.ssh/config` :
```
Host lamsade
    HostName ssh.lamsade.dauphine.fr
    Port 5022
    User nvrel
    IdentityFile ~/.ssh/id_ed25519_nvrel
    IdentitiesOnly yes
    IdentityAgent none

Host vmhadoopmaster
    HostName vmhadoopmaster.srv.lamsade.dauphine.fr
    User nvrel
    IdentityFile ~/.ssh/id_ed25519_nvrel
    IdentitiesOnly yes
    ProxyJump lamsade
```

Vérifier :
```bash
ssh -J lamsade vmhadoopmaster 'hostname && whoami'
```

### 2️⃣ Transfert du JAR et des données (SFTP via bastion)

Depuis la machine locale :
```bash
sftp -P 5022 -i ~/.ssh/id_ed25519_nvrel -o IdentitiesOnly=yes -o IdentityAgent=none \
  nvrel@ssh.lamsade.dauphine.fr <<'SFTP'
mkdir -p workspace/apps
mkdir -p workspace/data
cd workspace
put target/scala-2.12/flight-pipeline-assembly-0.1.0.jar apps/
put data/Flights/201201.csv data/
put data/Weather/201201hourly.txt data/
put data/wban_airport_timezone.csv data/
bye
SFTP
```

### 3️⃣ Connexion au cluster
```bash
ssh -J lamsade vmhadoopmaster
```

### 4️⃣ Préparer HDFS dans ton espace autorisé
```bash
export HDFS_BASE="/students/p6emiasd2025/nvrel"
export HDFS_DATA="$HDFS_BASE/flight-pipeline/data"
export HDFS_OUT="$HDFS_BASE/flight-pipeline/out-sample"
export HDFS_STAGING="$HDFS_BASE/.sparkStaging"

hdfs dfs -mkdir -p "$HDFS_DATA/Flights" "$HDFS_DATA/Weather" "$HDFS_OUT" "$HDFS_STAGING"
hdfs dfs -chmod 700 "$HDFS_STAGING"

hdfs dfs -put -f ~/workspace/data/201201.csv "$HDFS_DATA/Flights/"
hdfs dfs -put -f ~/workspace/data/201201hourly.txt "$HDFS_DATA/Weather/"
hdfs dfs -put -f ~/workspace/data/wban_airport_timezone.csv "$HDFS_DATA/"
```

### 5️⃣ Exécution sur YARN

a) Résoudre le chemin du JAR
```bash
APP_JAR=$(readlink -f ~/workspace/apps/flight-pipeline-assembly-0.1.0.jar)
```

b) Lancer `prepare` (échantillon janvier 2012)
```bash
spark-submit \
  --class flightpipeline.Main \
  --master yarn \
  --deploy-mode cluster \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.yarn.stagingDir="hdfs://$HDFS_STAGING" \
  --driver-memory 4g \
  --executor-memory 6g \
  --executor-cores 3 \
  --num-executors 6 \
  "$APP_JAR" \
  --mode=prepare \
  --flights="hdfs://$HDFS_DATA/Flights" \
  --weather="hdfs://$HDFS_DATA/Weather" \
  --airport="hdfs://$HDFS_DATA/wban_airport_timezone.csv" \
  --out="hdfs://$HDFS_OUT" \
  --hours=12 --lags=0 --sample-month=201201
```

c) Lancer `join`
```bash
spark-submit \
  --class flightpipeline.Main \
  --master yarn \
  --deploy-mode cluster \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.yarn.stagingDir="hdfs://$HDFS_STAGING" \
  --driver-memory 4g \
  --executor-memory 6g \
  --executor-cores 3 \
  --num-executors 6 \
  "$APP_JAR" \
  --mode=join \
  --flights="hdfs://$HDFS_DATA/Flights" \
  --weather="hdfs://$HDFS_DATA/Weather" \
  --airport="hdfs://$HDFS_DATA/wban_airport_timezone.csv" \
  --out="hdfs://$HDFS_OUT" \
  --hours=12 --lags=0
```

### 6️⃣ Vérification des sorties
```bash
hdfs dfs -ls "$HDFS_OUT/flight_clean.parquet/_delta_log"
hdfs dfs -ls "$HDFS_OUT/weather_clean.parquet/_delta_log"
hdfs dfs -ls "$HDFS_OUT/join_intermediate.parquet/_delta_log"
```

Sur ton poste, tu peux accéder à l'interface YARN :
```bash
ssh -f -N -L 18088:127.0.0.1:8088 vmhadoopmaster
```

Puis ouvrir 👉 http://localhost:18088

### 7️⃣ Script automatisé

Le script `run_cluster_sample.sh` (déjà inclus dans le projet) permet d'automatiser les étapes `prepare` et `join` :
```bash
# Syntaxe : ./run_cluster_sample.sh [YYYYMM] [LAGS]
./run_cluster_sample.sh 201201 0
```

Il gère :
- la création des dossiers HDFS,
- l'upload des fichiers,
- la configuration de staging YARN,
- les soumissions Spark (prepare puis join),
- et les vérifications finales.

---

## 📊 Résultats attendus

Sur HDFS (`/students/p6emiasd2025/nvrel/flight-pipeline/out-sample/`) :
```
├── flight_clean.parquet/
│   └── _delta_log/
├── weather_clean.parquet/
│   └── _delta_log/
├── airport_timezone_clean.parquet/
│   └── _delta_log/
├── join_intermediate.parquet/
│   └── _delta_log/
└── quality/
```

---

## 💡 Bonnes pratiques

- Toujours travailler dans l'espace HDFS personnel (`/students/p6emiasd2025/nvrel`).
- Ne jamais écrire sous `/user` ni `/tmp` du cluster.
- Éviter les `~` ou espaces dans les chemins Spark : utilise toujours des chemins absolus.
- Nettoyer les anciennes sorties avant un nouveau run :
```bash
  hdfs dfs -rm -r -f /students/p6emiasd2025/nvrel/flight-pipeline/out-sample
```
- Pour tester plusieurs mois :
```bash
  ./run_cluster_sample.sh 201202 0
```

---

## 📋 Licence

MIT License © 2025 — Nicolas Vrel