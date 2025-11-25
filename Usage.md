# Documentation du projet `flight-pipeline`

Ce document résume l’architecture du projet, le rôle des principales classes et la façon de lancer les différents modes (prepare / join / training / report), ainsi que la mécanique de journalisation des entraînements.

Le projet implémente, avec Spark + Scala, un pipeline proche de celui décrit dans l’article de Belcastro et al. (ACM TIST, 2014) sur la détection de retards météo (datasets D1–D4, métriques Acc / Reco / Recd, etc.).

---

## 1. Organisation générale du dépôt

Racine du dépôt :

- `build.sbt`  
  Dépendances du projet (Scala 2.12, Spark, Delta Lake, Typesafe Config…).

- `src/main/scala/flightpipeline/`  
  Code Scala de l’application.

- `src/main/resources/application.conf`  
  Configuration par défaut lue par `Main` (répertoires d’entrée/sortie, valeurs par défaut de lags, seuil de retard, dataset ciblé, jeu de features…).

- `scripts/`
    - `env-local.sh` : configuration locale (WSL / macOS) pour Spark (nombre de cœurs, mémoire, sérialisation, valeurs par défaut de certains paramètres comme `FP_RF_MAX_ROWS_PER_CLASS`).
    - `submit-local.sh` : script principal pour lancer le pipeline en local via `spark-submit`.

- `data/`  
  Attendu par défaut :
    - `data/Flights/*.csv` : vols bruts (BTS).
    - `data/Weather/*hourly.txt` : météo horaire.
    - `data/wban_airport_timezone.csv` : mapping station météo → aéroport + fuseaux horaires.

- `out/` (créé après exécution)  
  Contient toutes les sorties Delta / modèles / métriques :
    - `out/flight_clean`
    - `out/weather_clean`
    - `out/airport_timezone_clean`
    - `out/join_intermediate`
    - `out/join_flat_lags<L>`
    - `out/models/...`
    - `out/metrics/...` (métriques par run + journal `train_runs` + export CSV).

---

## 2. Principales classes et packages

### 2.1 `flightpipeline.Main`

Point d’entrée de l’application (classe passée à `spark-submit`).

Rôle :

- Charge la configuration par défaut `app` dans `application.conf`.
- Construit un objet `Args` en combinant :
    - les valeurs par défaut (dépôt),
    - les arguments de la ligne de commande (parse dans `Args.parse`).
- Crée la `SparkSession`.
- En fonction de `args.mode`, enchaîne les étapes :
    - `prepare` : nettoyage des données brutes vols + météo.
    - `quality` : contrôles qualité sur les tables clean.
    - `join` : jointure vols + météo (construction de Wo/Wd et des lags).
    - `training` / `train` : entraînement Random Forest + journalisation.
    - `report` : mode lecture de `train_runs` + export CSV (aucun recalcul).
    - `all` : exécute `prepare` → `quality` → `join` → `training`.

Main utilise aussi :

- `DataPaths` pour centraliser les chemins Delta.
- `ShowTrainingRuns` pour le mode `report`.
- Les différentes classes du package `stage` pour les étapes du pipeline.

### 2.2 `flightpipeline.config.Args`

Case class qui regroupe tous les paramètres d’exécution :

- `flightsDir` : chemin des vols bruts (glob).
- `weatherDir` : chemin des fichiers météo bruts.
- `airportCsv` : CSV de mapping station météo → aéroport / timezone.
- `outRoot` : racine des sorties `out`.
- `windowHours` : taille de la fenêtre temporelle du join météo (heures).
- `lags` : nombre de lags Wo/Wd (max 7 dans l’implémentation).
- `delayThresholdMinutes` : seuil de retard (ex. 60 minutes).
- `mode` : `all`, `prepare`, `join`, `training` (ou `train`), `quality`, `report`.
- `sampleMonth` : éventuellement un mois ciblé (`YYYYMM`) pour travailler sur un sous-ensemble.
- `delayDataset` : dataset de retards ciblé (`D1`, `D2`, `D3`, `D4`, `D_all`).
- `featureSet` : jeu de features pour l’entraînement RF :
    - `with-weather` : vols + lags météo complets.
    - `no-weather` : vols uniquement (baseline sans météo).
    - `article-weather` : sous-ensemble des features météo inspirées de l’article (variables cœur sur Wo/Wd).

`Args.parse` :

- Lit des arguments de la forme `--clé valeur` depuis la ligne de commande.
- Gère les alias :
    - `--delay-threshold` ou `--delay-threshold-min`.
    - `--delay-dataset` (normalise en `D1`–`D4` ou `D_all`).
- Valide les valeurs :
    - `--sample-month` doit respecter le format `YYYYMM`.
    - `--feature-set` doit être `with-weather`, `no-weather` ou `article-weather` (sinon exception).

### 2.3 `flightpipeline.io.DataPaths`

Classe utilitaire pour centraliser les chemins des tables Delta et des sorties :

- `flightCleanOut` : Delta `flight_clean`.
- `weatherCleanOut` : Delta `weather_clean`.
- `airportTimezoneCleanOut` : mapping station → aéroport / timezone.
- `joinIntermediateOut` : table Delta structurée pour l’entraînement.
- `joinFlatOut(lags)` : version “aplatie” (one row per flight) pour exploration / debug.
- `metrics`, `models`, etc., sous `outRoot`.

Cela évite de dupliquer des concaténations de chemins dans tout le code.

### 2.4 Package `flightpipeline.stage`

Ce package regroupe les grandes étapes du pipeline Spark.

- `FlightsRawToClean`  
  Lit les fichiers vols bruts (`data/Flights/*.csv`), applique un schéma cohérent, filtre et nettoie les lignes, et produit une table Delta `flight_clean`.  
  C’est la source unique pour les vols dans la suite du pipeline.

- `WeatherRawToClean`  
  Lit les fichiers météo bruts (`data/Weather/*hourly.txt`) ainsi que le CSV `wban_airport_timezone.csv`,  
  reconstruit des timestamps corrects (en tenant compte des timezones), normalise les champs météo et produit une table Delta `weather_clean`.  
  Le mapping sert aussi à restreindre les stations aux aéroports réellement présents dans `flight_clean`.

- `JoinFlightsWeather`  
  Étape clé de la construction de Wo (origine) et Wd (destination) :
    - joint `flight_clean` et `weather_clean` avec une fenêtre temporelle `windowHours`,
    - pour chaque vol, assemble deux tableaux :
        - `weather_origin` : Wo(t), Wo(t−1h), …, Wo(t−Lh),
        - `weather_dest`   : Wd(t), Wd(t−1h), …, Wd(t−Lh),
    - écrit le résultat dans :
        - `join_intermediate` (table Delta qui contient encore des arrays),
        - `join_flat_lags<L>` (version avec colonnes scalaire par lag, surtout utile en exploration).

  C’est cette table `join_intermediate` qui sera relue par `TrainRandomForest`.

- `QualityCheck`  
  Quelques analyses simples de qualité :
    - contrôles de cohérence entre `flight_clean`, `weather_clean` et le mapping aéroports,
    - statistiques descriptives sur les tables clean et jointes,
    - écrit des rapports sous `out/quality`.

- `TrainRandomForest`  
  Étape d’entraînement du modèle :
    - relit `join_intermediate`,
    - prépare les features (avec ou sans météo, ou “article-weather”),
    - construit un dataset équilibré pour l’entraînement/test (section 4.2 de l’article),
    - entraîne un Random Forest binaire,
    - calcule les métriques Acc / Reco / Recd (accuracy, rappel retards, rappel vols à l’heure),
    - journalise le run complet dans la table Delta `out/metrics/train_runs`
      via `TrainRunLogger`.

### 2.5 Package `flightpipeline.eval`

- `DelayDataset`  
  Documente les jeux D1–D4 décrits dans l’article (section 4.2, “Bad-weather delays detection”).  
  Le code d’entraînement exploite la même logique (mais via `delayDatasetId: String`) pour déterminer quels vols sont considérés comme “positifs” (label 1).

- `BinaryClassificationMetrics`  
  Petit helper pour calculer les métriques Acc / Reco / Recd à partir d’une matrice de confusion (TP/TN/FP/FN).  
  `TrainRandomForest` a une structure interne équivalente (`BinaryMetrics`) pour les mêmes métriques.

- `TrainRunLogger`  
  Composant central de journalisation :
    - définit un schéma Delta pour `out/metrics/train_runs`,
    - expose `logRun(...)` pour écrire une ligne par run d’entraînement,
    - gère l’append + évolution de schéma (option Delta `mergeSchema=true`),
    - expose `loadAllRuns` pour relire facilement l’historique.

### 2.6 Package `flightpipeline.report`

- `ShowTrainingRuns`  
  Utilisé par le mode `report` :
    - lit `out/metrics/train_runs`,
    - affiche un résumé dans les logs (tables triées par `ts`),
    - produit un CSV agrégé dans `out/metrics/train_runs_export`  
      pour pouvoir travailler facilement dans Excel / Numbers / Pandas.

---

## 3. Détail de l’étape d’entraînement (`TrainRandomForest`)

### 3.1 Paramètres de `TrainRandomForest`

Constructeur :

- `spark` : `SparkSession`.
- `joinIntermediatePath` : chemin Delta vers `join_intermediate`.
- `outRoot` : racine des sorties.
- `lags` : profondeur des lags Wo/Wd (cohérent avec la phase `join`).
- `delayThresholdMinutes` : seuil sur `ARR_DELAY_NEW` (60 min par défaut).
- `delayDatasetId` : `D1`, `D2`, `D3`, `D4` ou `ALL` :
    - **D1** : retards essentiellement dus à la météo ou NAS (quasi pas d’autres causes),
    - **D2** : retards météo ou retards où `NAS_DELAY >= seuil`,
    - **D3** : retards où météo ou NAS interviennent, même s’il y a d’autres causes,
    - **D4** : tous les vols retardés (ARR_DELAY_NEW ≥ seuil),
    - **ALL / D_all** : comportement large proche de D3.
- `featureSetId` : jeu de features :
    - `with-weather`    : vols + tous les lags météo disponibles,
    - `no-weather`      : vols uniquement (baseline sans météo),
    - `article-weather` : seulement les colonnes météo inspirées de l’article, par exemple :
        - température, humidité,
        - vent (direction/vitesse),
        - pression,
        - visibilité,
        - résumé de la couverture nuageuse,
        - scores WT sur les principaux phénomènes (pluie, orage, brouillard, etc.).

### 3.2 Échantillonnage équilibré (section 4.2 de l’article)

La méthode `buildBalancedTrainTest` :

1. Sépare les vols en deux classes :
    - positifs (label = 1.0) selon `positiveFilterForDataset(delayDatasetId, seuil)`,
    - vols “à l’heure” (ARR_DELAY_NEW < seuil) pour la classe 0.0.

2. Calcule :
    - taille de la classe positive,
    - taille de la classe “on-time”,
    - prend la classe minoritaire.

3. Détermine un plafond effectif par classe :

    - lit `FP_RF_MAX_ROWS_PER_CLASS` dans l’environnement (sinon 400000),
    - `effectivePerClass = min(minorityCount, maxRowsPerClass)`  
      → borne supérieure sur le nombre de vols par classe utilisés pour l’entraînement/test.

4. Tire aléatoirement `effectivePerClass` vols dans chaque classe (under-sampling).

5. Split chaque classe en train/test (75% / 25%).

6. Retourne :
    - un train équilibré (50% retards, 50% vols à l’heure),
    - un test équilibré,
    - les effectifs positifs/négatifs dans chaque split (utile pour le log).

### 3.3 Préparation des features

`prepareBaseDataset(joined, includeWeatherFeatures)` :

- lit les colonnes de vol indispensables :
    - `FL_DATE`, `CRS_DEP_TIMESTAMP`, `CRS_ELAPSED_TIME`,
    - `ORIGIN_AIRPORT_ID`, `DEST_AIRPORT_ID`,
    - `ARR_DELAY_NEW` (label de base).
- ajoute les colonnes de décomposition de retard :
    - `WEATHER_DELAY`, `NAS_DELAY`,
    - `HAS_WEATHER_DELAY`, `HAS_NAS_DELAY`,
      qui servent à définir D1–D4 (section 4.2).

- selon le jeu de features :
    - `with-weather` : déploie tous les champs numériques de `weather_origin` / `weather_dest`
      sur les `lags` premiers éléments (max 7).
    - `article-weather` : ne conserve que les champs de cœur de l’article sur les
      `lags` premiers éléments (max 7), toujours en séparant origine / destination :
      `orig_<champ>_lag0`, `orig_<champ>_lag1`, …, `dest_<champ>_lag0`, etc.
    - `no-weather` : n’ajoute aucune colonne issue des arrays météo.

- enrichit avec des variables temporelles simples :
    - `dep_hour` = heure de départ planifiée,
    - `dep_dow`  = jour de semaine.

- filtre les lignes sans `ARR_DELAY_NEW` (pas de label).

- remplace tous les NULL numériques par 0.0 pour éviter les erreurs dans `VectorAssembler`.

### 3.4 Sélection des features numériques

`inferFeatureColumns(df)` :

- exclut explicitement :
    - `label`,
    - `ARR_DELAY_NEW`, `FL_DATE`, `CRS_DEP_TIMESTAMP`,
    - `WEATHER_DELAY`, `NAS_DELAY`,
    - `HAS_WEATHER_DELAY`, `HAS_NAS_DELAY`  
      (utiles pour définir D1–D4 mais pas disponibles à la prédiction).

- garde toutes les colonnes numériques restantes (y compris les lags météo, `dep_hour`, etc.).

### 3.5 Entraînement Random Forest et métriques

`fitRandomForest(trainDF, featureCols)` :

- cast toutes les features en `Double`.
- assemble les features dans une colonne `features` (`VectorAssembler`).
- entraîne un `RandomForestClassifier` avec :
    - `numTrees = 100`,
    - `maxDepth = 15`,
    - `featureSubsetStrategy = "sqrt"`,
    - `subsamplingRate = 0.7`,
    - `minInstancesPerNode = 50`.

`computeMetrics(predictions, setName)` :

- calcule :
    - accuracy,
    - F1,
    - précision classe 1 (retards),
    - rappel classe 1 (Recd),
    - rappel classe 0 / spécificité (Reco),
    - matrice de confusion (TN, FP, FN, TP).

`saveTestMetrics(...)` :

- écrit une petite table Delta par modèle dans `out/metrics/rf_<dataset>_delay_<seuil>m`:
    - seuil de retard,
    - taille d’échantillon par classe,
    - nombre de features,
    - metrics complètes (Acc, F1, Recd, Reco, TN/FP/FN/TP).

---

## 4. Journalisation avec `TrainRunLogger` et consultation des résultats

### 4.1 Ce qui est loggué

`TrainRunLogger.logRun(...)` écrit une ligne par entraînement dans la table Delta :

- identifiants et contexte Spark :
    - `run_id`, `ts`, `location`, `spark_master`, `driver_memory`, `executor_memory`, `num_cores`.
- configuration données / modèle :
    - `delay_threshold_min`, `lags`, `window_hours`,
    - `sample_month`, `dataset_id` (D1–D4 / ALL),
    - dates de début / fin (`data_start`, `data_end`).
- tailles de jeux :
    - `n_joined`, `n_train`, `n_test`,
    - `n_train_pos`, `n_train_neg`, `n_test_pos`, `n_test_neg`.
- échantillonnage RF :
    - `rf_max_rows_per_class_limit` (valeur lue depuis `FP_RF_MAX_ROWS_PER_CLASS`),
    - `rf_effective_rows_per_class` (taille réelle de la classe minoritaire utilisée).
- durées d’entraînement :
    - `train_wall_time_sec`,
    - `train_driver_cpu_time_sec`.
- hyperparamètres :
    - `rf_num_trees`, `rf_max_depth`,
    - `rf_subsampling_rate`, `rf_feature_subset_strategy`.
- métriques train et test :
    - accuracy, F1, précision, rappel, spécificité, TN/FP/FN/TP.
- versioning / commentaire :
    - `git_commit`, `git_dirty`,
    - `comment` : concaténation de `FP_RUN_COMMENT` (si défini) et d’un commentaire technique
      (dataset, featureSet, lags, fenêtre, plafond RF).

La table est stockée sous :

- `out/metrics/train_runs` (format Delta).

### 4.2 Mode `show-runs` côté script

Le script `scripts/submit-local.sh` gère un mode spécial :

    bash scripts/submit-local.sh show-runs [N]

- lance un petit `spark-shell`,
- lit `out/metrics/train_runs`,
- affiche les `N` derniers runs (par défaut 50) avec les colonnes principales :
    - `ts`, `location`, `dataset_id`, `delay_threshold_min`, `lags`, `window_hours`,
      `n_train`, `n_test`, `test_accuracy`, `Recd`, `Reco`, `comment`.

Très pratique pour jeter un coup d’œil rapide aux essais locaux.

### 4.3 Mode `report` côté Scala

Depuis `Main`, lancer :

    bash scripts/submit-local.sh report

ou directement :

    spark-submit ... --mode report ...

En mode `report` :

- `ShowTrainingRuns.run(spark, args.outRoot)` est appelé ;
- le code lit `out/metrics/train_runs` et produit :
    - un affichage synthétique dans les logs,
    - un export CSV détaillé dans `out/metrics/train_runs_export`  
      (facile à ouvrir dans Excel / Numbers / Pandas).

C’est le point d’entrée naturel pour construire des figures “type article” (courbes Acc/Reco/Recd en fonction de D1–D4, etc.).

---

## 5. Lancement du pipeline et paramètres (via `submit-local.sh`)

### 5.1 Signature du script

Script principal (local, WSL ou macOS) :

    bash scripts/submit-local.sh MODE [LAGS] [DELAY_DATASET] [FEATURE_SET] [MONTH]

- `MODE` :
    - `all`       : prepare + quality + join + training,
    - `prepare`   : uniquement la préparation des tables clean,
    - `join`      : uniquement la jointure vols+météo,
    - `training`  : uniquement l’entraînement,
    - `train`     : alias de `training`,
    - `quality`   : relance les checks qualité,
    - `report`    : lecture de `train_runs` + export CSV,
    - `show-runs` : cas particulier géré au début du script (voir plus haut).

- `LAGS` : nombre de lags Wo/Wd à utiliser (par défaut 7).

- `DELAY_DATASET` : dataset ciblé pour l’entraînement :
    - `D1`, `D2`, `D3`, `D4`, `ALL`  
      (retranscrit ensuite côté Scala sous forme `D1`–`D4` ou `D_all`).

- `FEATURE_SET` : jeu de features :
    - `with-weather`,
    - `no-weather`,
    - `article-weather`.

- `MONTH` : si présent, restreint la lecture des données brutes à un seul mois au format `YYYYMM` :
    - flights : `data/Flights/YYYYMM.csv`,
    - météo  : `data/Weather/YYYYMMhourly.txt`.

### 5.2 Exemples de commandes

Pipeline complet, dataset D2, features complètes (vol + météo) :

    bash scripts/submit-local.sh all 7 D2 with-weather

Préparation + join seulement (utile pour itérer sur l’entraînement sans tout refaire) :

    bash scripts/submit-local.sh prepare
    bash scripts/submit-local.sh join 7

Entraînement uniquement, D2, modèle “article-weather” :

    bash scripts/submit-local.sh training 7 D2 article-weather

Baseline sans météo, D2 :

    bash scripts/submit-local.sh training 7 D2 no-weather

Voir les 100 derniers runs :

    bash scripts/submit-local.sh show-runs 100

Générer l’export CSV consolidé des runs :

    bash scripts/submit-local.sh report

---

## 6. Paramètres d’environnement et adaptation sur macOS

### 6.1 `scripts/env-local.sh`

Ce script contient tous les réglages locaux pour Spark :

- CPU / mémoire :
    - `FP_LOCAL_CORES` : nombre de cœurs utilisés en local (`local[N]`).
    - `FP_DRIVER_MEM`, `FP_EXEC_MEM` : mémoire du driver et de l’exécuteur, par exemple `24g`.
- Répertoires temporaires :
    - `FP_LOCAL_DIR` : répertoire utilisé par Spark pour les shuffles, spill, broadcast.
- Shuffle / parallélisme :
    - `FP_SHUFFLE_PARTS` : nombre de partitions shuffle.
    - `FP_DEFAULT_PAR` : parallélisme par défaut.
- I/O :
    - `FP_MAX_SPLIT` : taille maximale des blocs de fichiers (parquet/CSV).
    - `FP_BROADCAST` : seuil de broadcast pour les petites tables.
- Sérialisation / JVM :
    - `FP_SERIALIZER`, `FP_KRYO_MAXBUF`,
    - options JVM G1GC pour le driver / exécuteur.

Hyperparamètres contrôlés par l’environnement :

- `FP_RF_MAX_ROWS_PER_CLASS` :
    - plafond de lignes par classe pour l’échantillon équilibré (Random Forest),
    - par défaut 400000, ajustable à 1 000 000 ou plus sur une machine riche.
- `FP_TRAIN_LOCATION` :
    - tag de localisation logique (`local`, `cluster`, etc.) si besoin de surcharger la détection automatique.
- `FP_RUN_COMMENT` :
    - commentaire libre injecté dans la colonne `comment` de `train_runs`
      (complété par un commentaire technique via `TrainRandomForest`).

Spécifique Spark UI :

- `SPARK_LOCAL_IP`, `SPARK_PUBLIC_DNS`, `FP_UI_PORT`  
  utiles si plusieurs sessions Spark sont ouvertes en parallèle.

### 6.2 Notes spécifiques macOS

Sur un Mac puissant :

- Installer Java 11 (ou une version compatible avec Spark 3.5.x).
- Installer Spark (binaire précompilé) et définir `SPARK_HOME` :
    - par exemple `export SPARK_HOME=/Users/<login>/spark-3.5.2-bin-hadoop3`.
- Cloner le dépôt, se placer à la racine et adapter `env-local.sh` :
    - augmenter `FP_LOCAL_CORES` à la valeur souhaitée (`8`, `12`, `16`…),
    - ajuster `FP_DRIVER_MEM` / `FP_EXEC_MEM` en fonction de la RAM disponible (`24g`, `32g`, `48g`…),
    - pointer `FP_LOCAL_DIR` vers un disque rapide (SSD interne).
- Lancer ensuite :
    - `bash scripts/submit-local.sh all 7 D2 with-weather`
      pour vérifier que tout est fonctionnel.

---

## 7. Résumé de la prise en main

1. Cloner le dépôt et vérifier que `sbt` et `spark-submit` sont disponibles dans le PATH.
2. Adapter `scripts/env-local.sh` à la machine (Mac ou Linux) :
    - nombre de cœurs (`FP_LOCAL_CORES`),
    - mémoire (`FP_DRIVER_MEM` / `FP_EXEC_MEM`),
    - éventuellement `FP_RF_MAX_ROWS_PER_CLASS`.
3. Déposer les données brutes dans `data/Flights`, `data/Weather` et le CSV mapping.
4. Lancer un premier run complet :

       bash scripts/submit-local.sh all 7 D2 with-weather

5. Inspecter les métriques d’entraînement :
    - table Delta par modèle dans `out/metrics/rf_<dataset>_delay_60m`,
    - journal global dans `out/metrics/train_runs`.
6. Utiliser :

       bash scripts/submit-local.sh show-runs 50

   pour un aperçu rapide, puis :

       bash scripts/submit-local.sh report

   pour produire un CSV consolidé des runs dans `out/metrics/train_runs_export`.

7. Comparer facilement :
    - `with-weather` vs `no-weather` vs `article-weather`,
    - D1/D2/D3/D4,
    - différents seuils de retard si nécessaire.

Avec ces éléments, ton collègue doit pouvoir :

- comprendre la structure générale du projet,
- lancer des runs sur son Mac,
- interpréter les logs et les métriques dans Delta,
- et reconstruire des analyses type “article” (Acc / Reco / Recd selon D1–D4, impact de la météo, etc.).
