package flightpipeline.stage

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, NumericType, StructType}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory
import java.lang.management.ManagementFactory

import flightpipeline.eval.TrainRunLogger

/**
 * Entraînement d'un Random Forest binaire à partir de la table Delta
 * `join_intermediate` produite par `JoinFlightsWeather`.
 *
 * Label binaire :
 *   – 1.0 : vol "fortement retardé" (ARR_DELAY_NEW ≥ seuil)
 *   – 0.0 : vol à l’heure         (ARR_DELAY_NEW <  seuil)
 *
 * La sélection des vols positifs suit la définition des jeux D1–D4
 * de Belcastro et al., TIST 2014, section 4.2 ("Bad-weather delays detection").
 *
 * Paramètres importants :
 *   – delayThresholdMinutes : seuil sur ARR_DELAY_NEW (ex. 60 minutes),
 *   – lags                  : profondeur des séries météo Wo/Wd,
 *   – delayDatasetId        : "D1", "D2", "D3", "D4" ou "ALL",
 *   – featureSetId          : jeu de features utilisé côté modèle :
 *       • "with-weather"    : vol + toutes les features météo Wo/Wd disponibles,
 *       • "no-weather"      : uniquement les features de vol (baseline sans météo),
 *       • "article-weather" : uniquement les variables météo mentionnées
 *                             explicitement dans l’article (T, H, Wd, Ws, P, S, V, Di).
 *
 * Entrée :
 *   – joinIntermediatePath : Delta join_intermediate
 *
 * Sorties :
 *   – modèle MLlib  : outRoot/models/rf_<datasetId>_delay_<seuil>m
 *   – métriques     : outRoot/metrics/rf_<datasetId>_delay_<seuil>m (Delta, une ligne)
 *   – journal de run: outRoot/metrics/train_runs (Delta, une ligne par expérience)
 */
final class TrainRandomForest(
                               spark: SparkSession,
                               joinIntermediatePath: String,
                               outRoot: String,
                               lags: Int,
                               delayThresholdMinutes: Int = 60,
                               delayDatasetId: String = "D2",          // D1..D4 / ALL : jeux de retards (article section 4.2)
                               featureSetId: String = "with-weather"   // "with-weather" / "no-weather" / "article-weather"
                             ) {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Lecture de la limite d'échantillonnage par classe à partir
   * de l'environnement (variable FP_RF_MAX_ROWS_PER_CLASS).
   *
   * Cette valeur contrôle la taille maximale de chaque classe
   * dans l'échantillon équilibré décrit dans l’article
   * (section 4.2, Figure 4).
   *
   * Si la variable n’est pas définie ou invalide, repli
   * sur 400 000 lignes par classe, ce qui donne un dataset
   * de l’ordre du million de lignes après under‑sampling + split.
   */
  private val maxRowsPerClass: Long = {
    val raw = sys.env.get("FP_RF_MAX_ROWS_PER_CLASS")
    log.info(
      s"[Train] FP_RF_MAX_ROWS_PER_CLASS dans l'environnement = " +
        raw.getOrElse("non défini") + " (fallback Scala = 400000)"
    )

    raw.flatMap(v => scala.util.Try(v.toLong).toOption)
      .getOrElse(400000L)
  }

  /**
   * Choix du jeu de features :
   *
   *   – "with-weather"    : vol + toutes les features météo Wo/Wd produites
   *                         par `JoinFlightsWeather` (liste complète de selectWeatherColumns).
   *
   *   – "no-weather"      : modèle de référence qui ne voit que les caractéristiques
   *                         du vol (aéroports, horaires, etc.), comme dans la
   *                         section expérimentale où l’article montre qu’un modèle
   *                         sans météo fait déjà mieux que le hasard grâce à l’effet
   *                         "aéroport" et à quelques variables de vol.
   *
   *   – "article-weather" : jeu de features météo restreint aux variables décrites
   *                         explicitement dans la section 2.3 de l’article :
   *                           T (température),
   *                           H (humidité),
   *                           Wd/Ws (direction / vitesse du vent),
   *                           P (pression barométrique),
   *                           S (sky condition),
   *                           V (visibilité),
   *                           Di (descripteur de phénomènes météo).
   *
   * La valeur passée au constructeur (featureSetId) est normalisée ici
   * pour absorber les variations de casse et de séparateurs.
   */
  private val (normalizedFeatureSetId: String,
  includeWeatherFeatures: Boolean,
  articleWeatherFieldWhitelist: Option[Set[String]]) = {

    // Noyau des attributs météo correspondant aux variables citées dans l’article (section 2.3).
    // Mapping informel :
    //   - T  → DryBulbFarenheit
    //   - H  → RelativeHumidity
    //   - Ws → WindSpeed
    //   - Wd → WindDirection
    //   - P  → SeaLevelPressure
    //   - V  → Visibility
    //   - S  → variables de couverture nuageuse (sky_*)
    //   - Di → scores wt_score_* dérivés du descripteur de phénomènes
    val articleWeatherCoreFields: Set[String] = Set(
      // Thermiques / humidité / pression / vent / visibilité
      "DryBulbFarenheit",
      "RelativeHumidity",
      "WindSpeed",
      "WindDirection",
      "SeaLevelPressure",
      "Visibility",
      // Sky condition (nuages / convection)
      "sky_num_layers",
      "sky_min_altitude",
      "sky_max_altitude",
      "sky_mean_altitude",
      // Phénomènes météo (descripteur Di, pondéré)
      "wt_score_RA","wt_score_TS","wt_score_FG","wt_score_BR",
      "wt_score_FZ","wt_score_SN","wt_score_SH","wt_score_DZ"
    )

    val norm = Option(featureSetId)
      .getOrElse("with-weather")
      .trim
      .toLowerCase
      .replace(' ', '-')
      .replace('_', '-')

    norm match {
      case "with-weather" | "withweather" | "weather" | "full" =>
        log.info("[Train] Jeu de features = with-weather (vol + toutes les features météo Wo/Wd)")
        ("with-weather", true, None)

      case "no-weather" | "noweather" | "baseline" | "sans-meteo" | "sansmeteo" =>
        log.info("[Train] Jeu de features = no-weather (vol uniquement, baseline sans météo)")
        ("no-weather", false, None)

      case "article-weather" | "articleweather" | "article" | "paper-weather" =>
        log.info(
          "[Train] Jeu de features = article-weather " +
            "(vol + sous-ensemble météo aligné sur T, H, Wd, Ws, P, S, V, Di de l’article)"
        )
        ("article-weather", true, Some(articleWeatherCoreFields))

      case other =>
        log.warn(
          s"[Train] Jeu de features inconnu '$other', utilisation de 'with-weather' par défaut"
        )
        ("with-weather", true, None)
    }
  }

  /**
   * Point d’entrée principal de l’étape d’entraînement.
   *
   * Le DataFrame renvoyé contient les métriques de test
   * (accuracy, F1, rappel retardés, rappel vols à l’heure, matrice 2×2).
   */
  def run(): DataFrame = {
    val sc = spark.sparkContext
    sc.setJobDescription(
      s"RandomForest training (Th=${delayThresholdMinutes}m, dataset=$delayDatasetId, feature-set=$normalizedFeatureSetId)"
    )

    // Mesure des durées de run (article section 4.3 – coût d’apprentissage).
    val wallStartNs = System.nanoTime()

    log.info(
      s"[Train] Jeu de features sélectionné : $normalizedFeatureSetId " +
        "(with-weather = lags Wo/Wd complets, no-weather = sans météo, article-weather = sous-ensemble article)"
    )

    val threadBean  = ManagementFactory.getThreadMXBean
    val cpuStartOpt =
      if (threadBean.isCurrentThreadCpuTimeSupported)
        Some(threadBean.getCurrentThreadCpuTime)
      else None

    try {
      // ------------------------------------------------------------------
      // 1. Lecture de la table join_intermediate (résultat de JoinFlightsWeather)
      // ------------------------------------------------------------------
      log.info(s"[Train] Lecture de la table Delta join_intermediate depuis $joinIntermediatePath")
      val joined = spark.read.format("delta").load(joinIntermediatePath)

      val nJoined = joined.count()
      log.info(s"[Train] Dataset join_intermediate : $nJoined lignes, ${joined.columns.length} colonnes")

      // Bornes de période FL_DATE, utilisées pour documenter le run dans TrainRunLogger.
      val dateBounds = joined
        .agg(
          min(col("FL_DATE")).cast("date").as("min_date"),
          max(col("FL_DATE")).cast("date").as("max_date")
        )
        .first()

      val dataStart = dateBounds.getAs[java.sql.Date]("min_date")
      val dataEnd   = dateBounds.getAs[java.sql.Date]("max_date")

      // ------------------------------------------------------------------
      // 2. Préparation des features (vol + météo éventuelle)
      //
      // Référence : sections 2.2–3 de Belcastro et al.
      //   – "with-weather"      → extraction de toutes les séries météos Wo/Wd disponibles,
      //   – "no-weather"        → uniquement les features de vol,
      //   – "article-weather"   → seulement les variables T, H, Wd, Ws, P, S, V, Di.
      // ------------------------------------------------------------------
      val prepared = prepareBaseDataset(joined, includeWeatherFeatures)

      // ------------------------------------------------------------------
      // 3. Construction des jeux train/test équilibrés
      //
      // Référence : section 4.2 et Figure 4 ("balanced sampling").
      //   – définition de la classe positive selon D1..D4,
      //   – under-sampling pour équilibrer positifs / vols à l’heure,
      //   – split 75 % / 25 % dans chaque classe.
      // ------------------------------------------------------------------
      val (trainDF, testDF, nTrainPos, nTrainNeg, nTestPos, nTestNeg) =
        buildBalancedTrainTest(prepared)

      val nTrain = nTrainPos + nTrainNeg
      val nTest  = nTestPos + nTestNeg
      log.info(s"[Train] Split train/test équilibré : train=$nTrain lignes, test=$nTest lignes")

      trainDF.persist(StorageLevel.MEMORY_AND_DISK)
      testDF.persist(StorageLevel.MEMORY_AND_DISK)

      // ------------------------------------------------------------------
      // 4. Entraînement du Random Forest
      // ------------------------------------------------------------------
      val featureCols = inferFeatureColumns(trainDF)
      val model       = fitRandomForest(trainDF, featureCols)

      // ------------------------------------------------------------------
      // 5. Prédictions et métriques sur train et test
      // ------------------------------------------------------------------
      val trainPredictions = model.transform(trainDF)
      val testPredictions  = model.transform(testDF)

      val trainMetrics = computeMetrics(trainPredictions, "train")
      val testMetrics  = computeMetrics(testPredictions,  "test")

      // ------------------------------------------------------------------
      // 6. Sauvegarde modèle + métriques par seuil
      // ------------------------------------------------------------------
      val datasetTag = delayDatasetId.toUpperCase.replaceAll("\\s+", "")
      val metricsPath = s"$outRoot/metrics/rf_${datasetTag}_delay_${delayThresholdMinutes}m"
      val modelPath   = s"$outRoot/models/rf_${datasetTag}_delay_${delayThresholdMinutes}m"

      val metricsDF = saveTestMetrics(testMetrics, metricsPath, featureCols.length)

      log.info(s"[Train] Sauvegarde du modèle entraîné → $modelPath")
      model.write.overwrite().save(modelPath)

      // Extraction des hyperparamètres effectifs du RF.
      val rfStage = model.stages.collectFirst {
        case m: RandomForestClassificationModel => m
      }.getOrElse {
        throw new IllegalStateException("Stage RandomForestClassificationModel introuvable dans le pipeline")
      }

      // Fenêtre utilisée lors du join (fixée dans les scripts de soumission).
      val windowHoursForLog: Int =
        sys.env
          .get("FP_JOIN_WINDOW_HOURS")
          .flatMap(v => scala.util.Try(v.toInt).toOption)
          .getOrElse(12)

      // Durées mesurées.
      val wallTimeSec =
        (System.nanoTime() - wallStartNs) / 1e9

      val cpuTimeSec =
        cpuStartOpt
          .map(startNs => (threadBean.getCurrentThreadCpuTime - startNs) / 1e9)
          .getOrElse(Double.NaN)

      log.info(f"[Train] Durée mur (run()) ≈ $wallTimeSec%.1f s")
      if (!java.lang.Double.isNaN(cpuTimeSec))
        log.info(f"[Train] Durée CPU thread driver ≈ $cpuTimeSec%.1f s")

      // ------------------------------------------------------------------
      // 7. Journalisation de l’expérience dans TrainRunLogger
      // ------------------------------------------------------------------
      val location =
        if (sc.master.startsWith("local")) "local"
        else "cluster"

      // Taille effective par classe dans l’échantillon équilibré :
      // somme des positifs train et test = taille finale de la classe 1,
      // que l’on compare à la limite maxRowsPerClass.
      val effectivePerClass: Long = nTrainPos + nTestPos

      val commentFromEnv = sys.env.get("FP_RUN_COMMENT").filter(_.nonEmpty)
      val technicalComment =
        s"dataset=$datasetTag, featureSet=$normalizedFeatureSetId, " +
          s"lags=$lags, windowHours=$windowHoursForLog, " +
          s"maxRowsPerClass=$maxRowsPerClass"
      val combinedComment =
        (commentFromEnv.toSeq :+ technicalComment).mkString(" | ")

      TrainRunLogger.logRun(
        spark                     = spark,
        outRoot                   = outRoot,
        location                  = location,
        joinRowCount              = nJoined,
        nTrain                    = nTrain,
        nTest                     = nTest,
        nTrainPos                 = nTrainPos,
        nTrainNeg                 = nTrainNeg,
        nTestPos                  = nTestPos,
        nTestNeg                  = nTestNeg,
        rfMaxRowsPerClassLimit    = maxRowsPerClass,
        rfEffectiveRowsPerClass   = effectivePerClass,
        trainWallTimeSec          = wallTimeSec,
        trainDriverCpuTimeSec     = cpuTimeSec,
        delayThresholdMinutes     = delayThresholdMinutes,
        lags                      = lags,
        windowHours               = windowHoursForLog,
        sampleMonth               = Some("ALL"),            // à affiner si un mode échantillon est utilisé
        datasetId                 = Some(datasetTag),       // D1/D2/D3/D4/ALL dans la table de log
        dataStart                 = Option(dataStart),
        dataEnd                   = Option(dataEnd),
        rfNumTrees                = rfStage.getNumTrees,
        rfMaxDepth                = rfStage.getMaxDepth,
        rfSubsamplingRate         = rfStage.getSubsamplingRate,
        rfFeatureSubsetStrategy   = rfStage.getFeatureSubsetStrategy,
        trainAccuracy             = trainMetrics.accuracy,
        trainF1                   = trainMetrics.f1,
        trainPrecisionPos         = trainMetrics.precisionPos,
        trainRecallPos            = trainMetrics.recallPos,
        testAccuracy              = testMetrics.accuracy,
        testF1                    = testMetrics.f1,
        testPrecisionPos          = testMetrics.precisionPos,
        testRecallPos             = testMetrics.recallPos,
        testSpecificity           = testMetrics.specificity,
        testTN                    = testMetrics.tn,
        testFP                    = testMetrics.fp,
        testFN                    = testMetrics.fn,
        testTP                    = testMetrics.tp,
        gitCommit                 = None,
        gitDirty                  = None,
        comment                   = Some(combinedComment)
      )

      trainDF.unpersist()
      testDF.unpersist()

      metricsDF
    } finally {
      sc.setJobDescription(null)
      sc.clearJobGroup()
    }
  }

  // ---------------------------------------------------------------------------
  // Préparation des features (vol + météo Wo/Wd)
  // ---------------------------------------------------------------------------

  /**
   * Prépare le jeu de données pour le Random Forest.
   *
   *  – colonnes de vol (date, horaires, aéroports),
   *  – colonnes de retard (WEATHER_DELAY, NAS_DELAY, indicateurs),
   *  – lags météo à l’origine et à destination Wo/Wd (si includeWeatherFeatures = true),
   *  – variables temporelles simples (heure de départ, jour de la semaine),
   *  – remplacement systématique des NULL numériques par 0.0.
   *
   * Lorsque includeWeatherFeatures = false, cette méthode construit un
   * jeu de features "baseline" sans météo, comme dans les expériences
   * où l’article compare avec / sans variables météo.
   *
   * Lorsque normalizedFeatureSetId = "article-weather", seules les
   * variables météo correspondant à T, H, Wd, Ws, P, S, V et Di sont
   * conservées dans Wo/Wd (cf. section 2.3 de l’article).
   */
  private def prepareBaseDataset(
                                  joined: DataFrame,
                                  includeWeatherFeatures: Boolean
                                ): DataFrame = {

    val sc = spark.sparkContext
    sc.setJobDescription("TrainRF: préparation des colonnes de base")

    // Colonnes de vol indispensables au label et aux features.
    val baseCols = Seq(
      "FL_DATE",
      "CRS_DEP_TIMESTAMP",
      "CRS_ELAPSED_TIME",
      "ORIGIN_AIRPORT_ID",
      "DEST_AIRPORT_ID",
      "ARR_DELAY_NEW"
    ).filter(joined.columns.contains).map(col)

    // Colonnes de retard nécessaires à la définition des jeux D1..D4
    // (article section 4.2, "Bad-weather delays detection").
    val delayCols = Seq(
      "WEATHER_DELAY",
      "NAS_DELAY",
      "HAS_WEATHER_DELAY",
      "HAS_NAS_DELAY"
    ).filter(joined.columns.contains).map(col)

    // Extraction ou non des lags météo Wo/Wd selon le mode choisi.
    //
    // Référence article :
    //   – construction de Wo / Wd et des lags météo (sections 2.2–2.3),
    //   – comparaison "with weather" / "without weather" (section 4),
    //   – mode "article-weather" : restriction aux variables météo explicitement citées.
    val weatherCols: Seq[Column] =
      if (includeWeatherFeatures &&
        joined.columns.contains("weather_origin") &&
        joined.columns.contains("weather_dest")) {

        // Schéma d’un élément de weather_origin : struct<w_ts, ..., variables météo...>
        val originStruct =
          joined.schema("weather_origin").dataType
            .asInstanceOf[ArrayType]
            .elementType
            .asInstanceOf[StructType]

        // Candidats : tous les champs numériques de Wo/Wd (hors timestamp),
        // soit les features produites par selectWeatherColumns côté join.
        val candidateNumericFields: Seq[String] =
          originStruct.fields
            .filter(f => f.dataType.isInstanceOf[NumericType])
            .map(_.name)
            .filterNot(_ == "w_ts")

        // Si le mode est "article-weather", on restreint cette liste
        // au noyau T, H, Wd, Ws, P, S, V, Di.
        val numericFields: Seq[String] =
          articleWeatherFieldWhitelist match {
            case Some(whitelist) =>
              val filtered = candidateNumericFields.filter(whitelist.contains)
              if (filtered.isEmpty) {
                // Sécurité : si le schéma ne matche pas la whitelist (changement de noms),
                // on loggue et on retombe sur tous les champs disponibles.
                log.warn(
                  "[Train] Mode article-weather : aucun champ météo commun entre le schéma et la whitelist, " +
                    "utilisation de tous les champs numériques disponibles."
                )
                candidateNumericFields
              } else {
                log.info(
                  s"[Train] Champs météo utilisés en mode article-weather (Wo/Wd) : ${filtered.sorted.mkString(", ")}"
                )
                filtered
              }
            case None =>
              candidateNumericFields
          }

        // L’article montre qu’un historique d’environ 7 heures
        // capture déjà bien la dynamique des phénomènes météo.
        val effectiveLags = math.min(lags, 7)

        def lagCols(prefix: String, arrayCol: String): Seq[Column] =
          (0 until effectiveLags).flatMap { k =>
            numericFields.map { f =>
              col(arrayCol).getItem(k).getField(f).alias(s"${prefix}_${f}_lag$k")
            }
          }

        lagCols("orig", "weather_origin") ++ lagCols("dest", "weather_dest")
      } else {
        if (!includeWeatherFeatures) {
          // Mode baseline "sans météo" :
          // aucune colonne dérivée de weather_origin / weather_dest
          // n’est déployée. Le Random Forest ne voit que :
          //   – FL_DATE, CRS_DEP_TIMESTAMP, CRS_ELAPSED_TIME,
          //   – ORIGIN_AIRPORT_ID, DEST_AIRPORT_ID,
          //   – variables horaires dérivées (heure, jour de semaine),
          //   – colonnes de retard global (ARR_DELAY_NEW) et de cause.
          log.info("[Train] Jeu de features sans météo : aucun lag Wo/Wd n’est extrait")
        } else {
          log.warn("[Train] Colonnes weather_origin / weather_dest absentes : aucun lag météo utilisable")
        }
        Seq.empty[Column]
      }

    val baseWithWeather =
      joined
        .select((baseCols ++ delayCols ++ weatherCols): _*)
        // Variables temporelles simples (heure de départ, jour de la semaine).
        .withColumn("dep_hour", hour(col("CRS_DEP_TIMESTAMP")))
        .withColumn("dep_dow",  dayofweek(col("FL_DATE")))

    // Les lignes sans ARR_DELAY_NEW ne permettent pas de construire un label binaire.
    val cleaned = baseWithWeather.filter(col("ARR_DELAY_NEW").isNotNull)

    // Remplacement homogène des NULL numériques par 0.0 pour éviter les
    // problèmes dans VectorAssembler.
    val numericCols = cleaned.schema.fields.collect {
      case f if f.dataType.isInstanceOf[NumericType] => f.name
    }

    val cleanedNoNulls =
      if (numericCols.nonEmpty)
        cleaned.na.fill(0.0, numericCols)
      else
        cleaned

    val nClean = cleanedNoNulls.count()
    log.info(
      s"[Train] Dataset après préparation (includeWeather=$includeWeatherFeatures, featureSet=$normalizedFeatureSetId) : " +
        s"$nClean lignes, ${cleanedNoNulls.columns.length} colonnes"
    )

    cleanedNoNulls
  }

  // ---------------------------------------------------------------------------
  // Sélection des vols positifs pour D1..D4 (section 4.2)
  // ---------------------------------------------------------------------------

  /**
   * Condition qui identifie la classe positive (label = 1.0)
   * en fonction du jeu D1..D4 ciblé.
   *
   * Traduction en SQL des définitions de la section 4.2 :
   *   – D1 : retards presque entièrement dus à la météo ou NAS,
   *   – D2 : retards avec composante météo ou NAS_DELAY ≥ seuil,
   *   – D3 : retards où la météo ou NAS interviennent, même avec d’autres causes,
   *   – D4 : tous les retards (ARR_DELAY_NEW ≥ seuil),
   *   – ALL : comportement large similaire à D3.
   */
  private def positiveFilterForDataset(datasetId: String,
                                       threshold: Double): Column = {

    val delayMinutes   = coalesce(col("ARR_DELAY_NEW"), lit(0.0))
    val weatherMinutes = coalesce(col("WEATHER_DELAY"), lit(0.0))
    val nasMinutes     = coalesce(col("NAS_DELAY"), lit(0.0))

    val hasWeatherFlag =
      coalesce(col("HAS_WEATHER_DELAY").cast("boolean"), lit(false))
    val hasNasFlag =
      coalesce(col("HAS_NAS_DELAY").cast("boolean"), lit(false))

    val isDelayed = delayMinutes >= lit(threshold)

    val weatherOrNasMinutes = weatherMinutes + nasMinutes
    val hasWeatherOrNasFlag = hasWeatherFlag || hasNasFlag

    // Estimation simple des minutes imputables à d’autres causes.
    val otherCauseMinutes = delayMinutes - weatherOrNasMinutes

    datasetId.toUpperCase match {

      // D1 : retards expliqués quasi entièrement par météo/NAS.
      case "D1" =>
        isDelayed &&
          (weatherOrNasMinutes > lit(0.0) || hasWeatherOrNasFlag) &&
          (otherCauseMinutes <= lit(0.5))

      // D2 : retards météo OU retards avec NAS_DELAY ≥ seuil.
      case "D2" =>
        isDelayed && (
          weatherMinutes > lit(0.0) ||
            nasMinutes    >= lit(threshold)
          )

      // D3 : retards où météo ou NAS interviennent, quelle que soit la part
      //       des autres causes.
      case "D3" =>
        isDelayed && (
          weatherOrNasMinutes > lit(0.0) || hasWeatherOrNasFlag
          )

      // D4 : tous les vols retardés.
      case "D4" =>
        isDelayed

      // ALL : comportement large centré météo/NAS, proche de D3.
      case "ALL" | "D_ALL" =>
        isDelayed && (
          weatherOrNasMinutes > lit(0.0) || hasWeatherOrNasFlag
          )

      case other =>
        throw new IllegalArgumentException(
          s"[TrainRandomForest] Dataset de retard inconnu : '$other' " +
            s"(attendu : D1, D2, D3, D4 ou ALL)"
        )
    }
  }

  // ---------------------------------------------------------------------------
  // Construction des jeux train / test équilibrés
  // ---------------------------------------------------------------------------

  private def buildBalancedTrainTest(base: DataFrame)
  : (DataFrame, DataFrame, Long, Long, Long, Long) = {

    val sc = spark.sparkContext
    sc.setJobDescription("TrainRF: construction des jeux train/test équilibrés")

    val threshold = delayThresholdMinutes.toDouble

    val isOnTime = col("ARR_DELAY_NEW") < threshold
    val positiveCond = positiveFilterForDataset(delayDatasetId, threshold)

    val positives = base.filter(positiveCond)
    val onTime    = base.filter(isOnTime)

    val delayedCount = positives.count()
    val onTimeCount  = onTime.count()

    log.info(s"[Train] Candidats label=1 (dataset=$delayDatasetId) : $delayedCount lignes")
    log.info(s"[Train] Candidats label=0 (à l’heure)               : $onTimeCount lignes")

    val minorityCount = math.min(delayedCount, onTimeCount)

    if (minorityCount == 0L) {
      throw new IllegalStateException(
        s"Aucune paire de classes utilisable pour le seuil $delayThresholdMinutes " +
          s"(positifs=$delayedCount, onTime=$onTimeCount, dataset=$delayDatasetId)"
      )
    }

    // Plafond effectif par classe (voir commentaire dans TrainRunLogger).
    val effectivePerClass: Long =
      if (maxRowsPerClass <= 0L) minorityCount
      else math.min(minorityCount, maxRowsPerClass)

    log.info(
      s"[Train] Classe minoritaire : $minorityCount lignes ; " +
        s"plafond demandé maxRowsPerClass=$maxRowsPerClass ; " +
        s"plafond effectif par classe=$effectivePerClass"
    )

    require(effectivePerClass > 0L, "effectivePerClass doit être > 0")

    // Tirage pseudo-aléatoire de target lignes sans remise.
    def sampleFixed(df: DataFrame, target: Long, seed: Long): DataFrame = {
      val n = df.count()
      if (n <= target) {
        df
      } else {
        val fraction = math.min(1.0, target.toDouble * 1.2 / n.toDouble)
        df.sample(withReplacement = false, fraction = fraction, seed = seed)
          .limit(target.toInt)
      }
    }

    val delayedBalanced = sampleFixed(positives, effectivePerClass, seed = 11L)
      .withColumn("label", lit(1.0))

    val onTimeBalanced  = sampleFixed(onTime,    effectivePerClass, seed = 22L)
      .withColumn("label", lit(0.0))

    log.info(
      s"[Train] Après under-sampling : " +
        s"label=1 → ${delayedBalanced.count()} lignes, " +
        s"label=0 → ${onTimeBalanced.count()} lignes"
    )

    val trainFraction = 0.75
    val testFraction  = 0.25

    val Array(delayedTrain, delayedTest) =
      delayedBalanced.randomSplit(Array(trainFraction, testFraction), seed = 101L)

    val Array(onTimeTrain, onTimeTest)  =
      onTimeBalanced.randomSplit(Array(trainFraction, testFraction), seed = 202L)

    val train = delayedTrain.unionByName(onTimeTrain)
    val test  = delayedTest.unionByName(onTimeTest)

    val nTrainPos = delayedTrain.count()
    val nTrainNeg = onTimeTrain.count()
    val nTestPos  = delayedTest.count()
    val nTestNeg  = onTimeTest.count()

    log.info(
      s"[Train] Jeu d’apprentissage : total=${train.count()} " +
        s"(label=1 → $nTrainPos, label=0 → $nTrainNeg)"
    )
    log.info(
      s"[Train] Jeu de test        : total=${test.count()} " +
        s"(label=1 → $nTestPos, label=0 → $nTestNeg)"
    )

    (train, test, nTrainPos, nTrainNeg, nTestPos, nTestNeg)
  }

  // ---------------------------------------------------------------------------
  // Sélection des features numériques
  // ---------------------------------------------------------------------------

  /**
   * Colonnes numériques utilisées comme features par le Random Forest.
   *
   * Référence article :
   *   – le but est d’évaluer l’apport des données météo Wo/Wd par rapport
   *     à un modèle basé uniquement sur les caractéristiques du vol,
   *     sans fuite d’information depuis les causes de retard déjà observées.
   *
   * Important :
   *   – on exclut explicitement toutes les colonnes qui contiennent
   *     le label ou des informations de retard observé :
   *       ARR_DELAY_NEW, WEATHER_DELAY, NAS_DELAY,
   *       HAS_WEATHER_DELAY, HAS_NAS_DELAY.
   *     Ces variables servent à définir D1–D4 (section 4.2), mais ne doivent
   *     pas être utilisées comme features, sous peine de fuite de label.
   */
  private def inferFeatureColumns(df: DataFrame): Array[String] = {
    // Colonnes qui ne doivent pas être utilisées comme features :
    //  - label binaire (target),
    //  - délai observé (ARR_DELAY_NEW),
    //  - date / timestamp bruts (on préfère dep_hour / dep_dow),
    //  - colonnes de décomposition du retard par cause
    //    (WEATHER_DELAY, NAS_DELAY, HAS_WEATHER_DELAY, HAS_NAS_DELAY),
    //    qui servent à définir D1–D4 (article section 4.2)
    //    mais ne sont pas disponibles "en temps réel" pour la prédiction.
    val excluded = Set(
      "label",
      "ARR_DELAY_NEW",
      "FL_DATE",
      "CRS_DEP_TIMESTAMP",
      "WEATHER_DELAY",
      "NAS_DELAY",
      "HAS_WEATHER_DELAY",
      "HAS_NAS_DELAY"
    )

    val numericCols = df.schema.fields.collect {
      case f
        if !excluded.contains(f.name) &&
          (f.dataType.isInstanceOf[NumericType] ||
            f.dataType.typeName == "double" ||
            f.dataType.typeName == "float"  ||
            f.dataType.typeName == "integer"||
            f.dataType.typeName == "long"   ||
            f.dataType.typeName == "short"  ||
            f.dataType.typeName == "byte") =>
        f.name
    }

    log.info(s"[Train] Nombre de colonnes numériques retenues comme features : ${numericCols.length}")
    numericCols.sorted
  }

  // ---------------------------------------------------------------------------
  // Entraînement du Random Forest
  // ---------------------------------------------------------------------------

  private def fitRandomForest(trainDF: DataFrame, featureCols: Array[String]): PipelineModel = {
    val sc = spark.sparkContext
    sc.setJobDescription("TrainRF: entraînement du Random Forest")

    val casted = featureCols.foldLeft(trainDF) { (df, c) =>
      df.withColumn(c, col(c).cast("double"))
    }

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
      .setHandleInvalid("keep")

    // Hyperparamètres inspirés de l’article (section 4.3) :
    // grand nombre d’arbres, profondeur raisonnable, sous-échantillonnage.
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(100)
      .setMaxDepth(15)
      .setFeatureSubsetStrategy("sqrt")
      .setSubsamplingRate(0.7)
      .setMinInstancesPerNode(50)
      .setSeed(42L)

    val pipeline = new Pipeline().setStages(Array(assembler, rf))

    log.info(
      s"[Train] Démarrage RF : numTrees=${rf.getNumTrees}, " +
        s"maxDepth=${rf.getMaxDepth}, subsamplingRate=${rf.getSubsamplingRate}"
    )

    val model = pipeline.fit(casted)

    log.info("[Train] Entraînement Random Forest terminé")
    model
  }

  // ---------------------------------------------------------------------------
  // Évaluation (Acc, Reco, Recd) et écriture des métriques
  // ---------------------------------------------------------------------------

  private case class BinaryMetrics(
                                    accuracy: Double,
                                    f1: Double,
                                    precisionPos: Double,
                                    recallPos: Double,
                                    specificity: Double,
                                    tn: Long,
                                    fp: Long,
                                    fn: Long,
                                    tp: Long
                                  )

  private def computeMetrics(predictions: DataFrame, setName: String): BinaryMetrics = {
    val sc = spark.sparkContext
    sc.setJobDescription(s"TrainRF: évaluation ($setName)")

    val accuracyEval = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val f1Eval = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("f1")

    val accuracy = accuracyEval.evaluate(predictions)
    val f1       = f1Eval.evaluate(predictions)

    // Matrice de confusion 2×2 (section 2.4).
    val confusion = predictions
      .groupBy("label", "prediction")
      .agg(count(lit(1)).as("count"))
      .collect()
      .map { row =>
        val label = row.getDouble(0).toInt
        val pred  = row.getDouble(1).toInt
        val cnt   = row.getLong(2)
        ((label, pred), cnt)
      }
      .toMap

    def c(y: Int, yhat: Int): Long = confusion.getOrElse((y, yhat), 0L)

    val tp = c(1, 1).toDouble
    val tn = c(0, 0).toDouble
    val fp = c(0, 1).toDouble
    val fn = c(1, 0).toDouble

    val precisionPos = if (tp + fp > 0.0) tp / (tp + fp) else 0.0
    val recallPos    = if (tp + fn > 0.0) tp / (tp + fn) else 0.0
    val specificity  = if (tn + fp > 0.0) tn / (tn + fp) else 0.0

    log.info(f"[Train][$setName] Accuracy                      : $accuracy%1.4f")
    log.info(f"[Train][$setName] F1                            : $f1%1.4f")
    log.info(f"[Train][$setName] Precision (classe 1)          : $precisionPos%1.4f")
    log.info(f"[Train][$setName] Recall (classe 1, Recd)       : $recallPos%1.4f")
    log.info(f"[Train][$setName] Spécificité (classe 0, Reco)  : $specificity%1.4f")
    log.info(s"[Train][$setName] Matrice de confusion (label,pred,count) : $confusion")

    BinaryMetrics(
      accuracy     = accuracy,
      f1           = f1,
      precisionPos = precisionPos,
      recallPos    = recallPos,
      specificity  = specificity,
      tn = c(0, 0),
      fp = c(0, 1),
      fn = c(1, 0),
      tp = c(1, 1)
    )
  }

  private def saveTestMetrics(
                               testMetrics: BinaryMetrics,
                               metricsPath: String,
                               numFeatures: Int
                             ): DataFrame = {
    import spark.implicits._

    val metricsDF = Seq(
      (
        delayThresholdMinutes,
        maxRowsPerClass,
        numFeatures,
        testMetrics.accuracy,
        testMetrics.f1,
        testMetrics.precisionPos,
        testMetrics.recallPos,
        testMetrics.specificity,
        testMetrics.tn,
        testMetrics.fp,
        testMetrics.fn,
        testMetrics.tp
      )
    ).toDF(
      "delay_threshold_minutes",
      "max_rows_per_class",
      "num_features",
      "accuracy",
      "f1",
      "precision_pos",
      "recall_pos",
      "specificity",
      "tn",
      "fp",
      "fn",
      "tp"
    )

    log.info(s"[Train] Écriture des métriques de test dans $metricsPath")
    metricsDF.write.format("delta").mode("overwrite").save(metricsPath)

    metricsDF
  }
}
