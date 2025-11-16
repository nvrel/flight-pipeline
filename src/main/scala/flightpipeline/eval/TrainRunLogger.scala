package flightpipeline.eval

import java.sql.{Date, Timestamp}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/**
 * Journal structuré des expériences d'entraînement Random Forest.
 *
 * Objectif principal :
 *   – garder une trace exploitable de chaque run
 *   – faciliter la rédaction du rapport (tableaux de synthèse, comparaisons)
 *
 * Chaque ligne correspond à un entraînement décrit par :
 *   – le contexte d'exécution Spark,
 *   – la configuration de données (fenêtre, lags, jeu D1..D4, période),
 *   – les hyperparamètres du Random Forest,
 *   – les tailles de jeux (train/test, positifs/négatifs),
 *   – les métriques principales (Acc, Reco, Recd, F1) comme dans l’article
 *     Belcastro et al., TIST 2014, en particulier la section 4.2–4.4,:contentReference[oaicite:0]{index=0}
 *   – un commentaire libre (par exemple FP_RUN_COMMENT).
 */
object TrainRunLogger {

  private val log = LoggerFactory.getLogger(getClass)

  // Schéma "canonique" de la table de log.
  // Delta permettra d’ajouter de nouveaux champs via mergeSchema=true
  // sans casser les runs existants.
  val schema: StructType = StructType(Seq(
    StructField("run_id", StringType, nullable = false),
    StructField("ts", TimestampType, nullable = false),

    // Contexte d'exécution Spark
    StructField("location", StringType, nullable = true),          // "local" ou "cluster"
    StructField("spark_master", StringType, nullable = true),
    StructField("driver_memory", StringType, nullable = true),
    StructField("executor_memory", StringType, nullable = true),
    StructField("num_cores", IntegerType, nullable = true),

    // Contexte de données / configuration du modèle
    // (référence à la définition des jeux D1..D4 en section 4.2)
    StructField("delay_threshold_min", IntegerType, nullable = false),
    StructField("lags", IntegerType, nullable = false),
    StructField("window_hours", IntegerType, nullable = true),
    StructField("sample_month", StringType, nullable = true),      // ex. "201205" ou "ALL"
    StructField("dataset_id", StringType, nullable = true),        // D1, D2, D3, D4, ALL…
    StructField("data_start", DateType, nullable = true),
    StructField("data_end", DateType, nullable = true),

    // Tailles de jeux (cf. article, section 4.2 – "Target Data Creation")
    StructField("n_joined", LongType, nullable = true),
    StructField("n_train", LongType, nullable = true),
    StructField("n_test", LongType, nullable = true),
    StructField("n_train_pos", LongType, nullable = true),
    StructField("n_train_neg", LongType, nullable = true),
    StructField("n_test_pos", LongType, nullable = true),
    StructField("n_test_neg", LongType, nullable = true),

    // Plafond théorique et taille réellement utilisée pour chaque classe.
    // Permet de voir si FP_RF_MAX_ROWS_PER_CLASS a été atteint.
    StructField("rf_max_rows_per_class_limit", LongType, nullable = true),
    StructField("rf_effective_rows_per_class", LongType, nullable = true),

    // Durées d’entraînement (section 4.3 – coût de l’algorithme)
    // Durée mur de la méthode run() et temps CPU du thread driver.
    StructField("train_wall_time_sec", DoubleType, nullable = true),
    StructField("train_driver_cpu_time_sec", DoubleType, nullable = true),

    // Hyperparamètres Random Forest (référence section 4.3)
    StructField("rf_num_trees", IntegerType, nullable = true),
    StructField("rf_max_depth", IntegerType, nullable = true),
    StructField("rf_subsampling_rate", DoubleType, nullable = true),
    StructField("rf_feature_subset_strategy", StringType, nullable = true),

    // Métriques train
    StructField("train_accuracy", DoubleType, nullable = true),
    StructField("train_f1", DoubleType, nullable = true),
    StructField("train_precision_pos", DoubleType, nullable = true),
    StructField("train_recall_pos", DoubleType, nullable = true),

    // Métriques test (Acc / Reco / Recd au sens de l’article, Fig. 8–9)
    StructField("test_accuracy", DoubleType, nullable = true),     // Acc
    StructField("test_f1", DoubleType, nullable = true),
    StructField("test_precision_pos", DoubleType, nullable = true),
    StructField("test_recall_pos", DoubleType, nullable = true),   // Recd (retards)
    StructField("test_specificity", DoubleType, nullable = true),  // Reco (vols à l’heure)

    // Matrice de confusion sur test
    StructField("test_tn", LongType, nullable = true),
    StructField("test_fp", LongType, nullable = true),
    StructField("test_fn", LongType, nullable = true),
    StructField("test_tp", LongType, nullable = true),

    // Versioning du code
    StructField("git_commit", StringType, nullable = true),
    StructField("git_dirty", BooleanType, nullable = true),

    // Commentaire libre (incluant FP_RUN_COMMENT)
    StructField("comment", StringType, nullable = true)
  ))

  /**
   * Construit un DataFrame Spark à partir d’une seule ligne de valeurs.
   *
   * Utilisation de Row + StructType plutôt que des tuples Scala pour
   * éviter la limite des 22 éléments.
   */
  private def singleRowDataFrame(
                                  spark: SparkSession,
                                  values: Seq[Any]
                                ): DataFrame = {
    require(
      values.length == schema.length,
      s"TrainRunLogger: ${schema.length} valeurs attendues, ${values.length} reçues"
    )

    val row = Row.fromSeq(values)
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(row)),
      schema
    )
  }

  /**
   * Enregistre une expérience d’entraînement dans la table Delta
   * out/metrics/train_runs.
   *
   * Tous les paramètres sont "plats" pour garder un appel simple
   * depuis `TrainRandomForest.run`.
   */
  def logRun(
              spark: SparkSession,
              outRoot: String,

              // Contexte global d’exécution
              location: String,          // "local" ou "cluster"
              joinRowCount: Long,        // taille de join_intermediate
              nTrain: Long,
              nTest: Long,
              nTrainPos: Long,
              nTrainNeg: Long,
              nTestPos: Long,
              nTestNeg: Long,
              rfMaxRowsPerClassLimit: Long,
              rfEffectiveRowsPerClass: Long,
              trainWallTimeSec: Double,
              trainDriverCpuTimeSec: Double,

              // Configuration "scientifique" de l’expérience
              delayThresholdMinutes: Int,
              lags: Int,
              windowHours: Int,
              sampleMonth: Option[String],
              datasetId: Option[String],   // D1/D2/D3/D4/ALL
              dataStart: Option[Date],
              dataEnd: Option[Date],

              // Hyperparamètres Random Forest
              rfNumTrees: Int,
              rfMaxDepth: Int,
              rfSubsamplingRate: Double,
              rfFeatureSubsetStrategy: String,

              // Métriques train
              trainAccuracy: Double,
              trainF1: Double,
              trainPrecisionPos: Double,
              trainRecallPos: Double,

              // Métriques test
              testAccuracy: Double,
              testF1: Double,
              testPrecisionPos: Double,
              testRecallPos: Double,
              testSpecificity: Double,
              testTN: Long,
              testFP: Long,
              testFN: Long,
              testTP: Long,

              // Versioning
              gitCommit: Option[String],
              gitDirty: Option[Boolean],

              // Commentaire libre (ex: FP_RUN_COMMENT + info techniques)
              comment: Option[String]
            ): DataFrame = {

    val nowTs = new Timestamp(System.currentTimeMillis())

    val sc   = spark.sparkContext
    val conf = sc.getConf

    val master      = sc.master
    val driverMem   = conf.getOption("spark.driver.memory")
    val executorMem = conf.getOption("spark.executor.memory")

    // Estimation du nombre de cœurs vus par le job Spark.
    val numCores =
      conf.getOption("spark.executor.cores")
        .orElse(conf.getOption("spark.cores.max"))
        .flatMap(v => scala.util.Try(v.toInt).toOption)
        .getOrElse(Runtime.getRuntime.availableProcessors())

    val runId = java.util.UUID.randomUUID().toString

    val values: Seq[Any] = Seq(
      runId,
      nowTs,

      // Contexte Spark
      location,
      master,
      driverMem.orNull,
      executorMem.orNull,
      numCores,

      // Configuration scientifique
      delayThresholdMinutes,
      lags,
      windowHours,
      sampleMonth.orNull,
      datasetId.orNull,
      dataStart.orNull,
      dataEnd.orNull,

      // Tailles de jeux
      joinRowCount,
      nTrain,
      nTest,
      nTrainPos,
      nTrainNeg,
      nTestPos,
      nTestNeg,

      // Taille de l’échantillon équilibré
      rfMaxRowsPerClassLimit,
      rfEffectiveRowsPerClass,

      // Durées d’entraînement
      trainWallTimeSec,
      trainDriverCpuTimeSec,

      // Hyperparamètres RF
      rfNumTrees,
      rfMaxDepth,
      rfSubsamplingRate,
      rfFeatureSubsetStrategy,

      // Métriques train
      trainAccuracy,
      trainF1,
      trainPrecisionPos,
      trainRecallPos,

      // Métriques test
      testAccuracy,
      testF1,
      testPrecisionPos,
      testRecallPos,
      testSpecificity,
      testTN,
      testFP,
      testFN,
      testTP,

      // Versioning + commentaire
      gitCommit.orNull,
      gitDirty.getOrElse(false),
      comment.orNull
    )

    val df = singleRowDataFrame(spark, values)

    val logPath = s"$outRoot/metrics/train_runs"

    // Simple message pour savoir si la table existait déjà.
    val fs      = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val pathObj = new Path(logPath)
    val exists  = fs.exists(pathObj)

    if (exists)
      log.info(s"[TrainRunLogger] Table de log déjà présente, ajout d’une nouvelle entrée.")
    else
      log.info(s"[TrainRunLogger] Création de la table de log Delta dans $logPath")

    // Écriture avec mergeSchema=true pour laisser évoluer le schéma dans le temps.
    df.write
      .format("delta")
      .mode("append")
      .option("mergeSchema", "true")
      .save(logPath)

    log.info(s"[TrainRunLogger] Run $runId journalisé dans $logPath")
    df
  }

  /**
   * Chargement de l’historique complet des runs.
   *
   * Sert de base au mode "report" pour construire :
   *   – un tableau synthétique dans les logs,
   *   – un export CSV exploitable dans le rapport.
   */
  def loadAllRuns(spark: SparkSession, outRoot: String): DataFrame = {
    val path = s"$outRoot/metrics/train_runs"
    spark.read.format("delta").load(path).orderBy(col("ts").desc)
  }
}
