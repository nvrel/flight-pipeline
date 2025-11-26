package flightpipeline.stage

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Column, DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, NumericType, StructType, DoubleType, FloatType}
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
   * Noyau des attributs météo correspondant aux variables citées dans l’article (section 2.3).
   * Mapping informel (d’après les noms de colonnes disponibles) :
   *   - T  → DryBulbFarenheit
   *   - H  → RelativeHumidity
   *   - Ws → WindSpeed
   *   - Wd → WindDirection
   *   - P  → SeaLevelPressure
   *   - V  → Visibility (string dans les données ; non utilisée telle quelle ici)
   *   - S  → variables de couverture nuageuse (sky_*),
   *   - Di → scores wt_score_* dérivés du descripteur de phénomènes.
   *
   * Ce set sert de documentation pour le mode "article-weather" et peut être
   * utilisé si l’on souhaite filtrer explicitement les champs météo à l’avenir.
   */
  private val articleWeatherCoreFields: Set[String] = Set(
    // Thermiques / humidité / pression / vent
    "DryBulbFarenheit",
    "RelativeHumidity",
    "WindSpeed",
    "WindDirection",
    "SeaLevelPressure",
    // S : proxy via paramètres de nébulosité
    "sky_num_layers",
    "sky_min_altitude",
    "sky_max_altitude",
    "sky_mean_altitude",
    // Di : phénomènes météo (pluie, neige, orages, brouillard, etc.)
    "wt_score_RA","wt_score_TS","wt_score_FG","wt_score_BR",
    "wt_score_FZ","wt_score_SN","wt_score_SH","wt_score_DZ"
  )

  /**
   * Modélisation explicite des trois jeux de features possibles.
   *
   *   – WithWeather    : vol + toutes les features météo Wo/Wd produites
   *                      par `JoinFlightsWeather` (liste complète de selectWeatherColumns).
   *
   *   – NoWeather      : modèle de référence qui ne voit que les caractéristiques
   *                      du vol (aéroports, horaires, etc.), comme dans la
   *                      section expérimentale où l’article montre qu’un modèle
   *                      sans météo fait déjà mieux que le hasard grâce à l’effet
   *                      "aéroport" et à quelques variables de vol.
   *
   *   – ArticleWeather : jeu de features météo restreint aux variables décrites
   *                      explicitement dans la section 2.3 de l’article :
   *                        T (température),
   *                        H (humidité),
   *                        Wd/Ws (direction / vitesse du vent),
   *                        P (pression barométrique),
   *                        S (sky condition),
   *                        V (visibilité),
   *                        Di (descripteur de phénomènes météo).
   *
   * La valeur passée au constructeur (featureSetId) est normalisée ici
   * pour absorber les variations de casse et de séparateurs, puis
   * convertie en une instance de FeatureSetMode.
   */
  private sealed trait FeatureSetMode {
    def id: String
    def includeWeather: Boolean
  }
  private case object WithWeather extends FeatureSetMode {
    val id: String = "with-weather"
    val includeWeather: Boolean = true
  }
  private case object NoWeather extends FeatureSetMode {
    val id: String = "no-weather"
    val includeWeather: Boolean = false
  }
  private case object ArticleWeather extends FeatureSetMode {
    val id: String = "article-weather"
    val includeWeather: Boolean = true
  }

  private object FeatureSetMode {

    /**
     * Normalise la chaîne passée au constructeur (featureSetId) et la
     * mappe sur l’un des trois modes supportés.
     */
    def fromId(raw: String): FeatureSetMode = {
      val norm = Option(raw)
        .getOrElse("with-weather")
        .trim
        .toLowerCase
        .replace(' ', '-')
        .replace('_', '-')

      norm match {
        case "with-weather" | "withweather" | "weather" | "full" =>
          log.info("[Train] Jeu de features = with-weather (vol + toutes les features météo Wo/Wd)")
          WithWeather

        case "no-weather" | "noweather" | "baseline" | "sans-meteo" | "sansmeteo" =>
          log.info("[Train] Jeu de features = no-weather (vol uniquement, baseline sans météo)")
          NoWeather

        case "article-weather" | "articleweather" | "article" | "paper-weather" =>
          log.info(
            "[Train] Jeu de features = article-weather " +
              "(vol + sous-ensemble météo aligné sur T, H, Wd, Ws, P, S, V, Di de l’article)"
          )
          log.info(
            s"[Train] Noyau de champs météo (article-weather) : " +
              articleWeatherCoreFields.mkString(", ")
          )
          ArticleWeather

        case other =>
          log.warn(
            s"[Train] Jeu de features inconnu '$other', utilisation de 'with-weather' par défaut"
          )
          WithWeather
      }
    }
  }

  /** Mode de features utilisé pour ce run (normalisé). */
  private val mode: FeatureSetMode = FeatureSetMode.fromId(featureSetId)

  /** Identifiant normalisé du jeu de features (utilisé dans les logs et la table de runs). */
  private val normalizedFeatureSetId: String = mode.id

  /**
   * Hyperparamètres du Random Forest, éventuellement adaptés
   * au jeu de features (with-weather / no-weather / article-weather).
   *
   * L’idée est de conserver l’esprit de l’article (section 4.3) :
   *   – nombre d’arbres suffisant,
   *   – profondeur contrôlée,
   *   – sous-échantillonnage,
   * tout en tenant compte de la taille du vecteur de features :
   *   – petit pour no-weather,
   *   – plus riche pour with-weather / article-weather.
   */
  private case class RfHyperParams(
                                    numTrees: Int,
                                    maxDepth: Int,
                                    minInstancesPerNode: Int,
                                    subsamplingRate: Double,
                                    featureSubsetStrategy: String = "sqrt",
                                    seed: Long = 42L
                                  )

  /**
   * Table de correspondance (mode -> hyperparamètres RF).
   *
   * Ces valeurs sont des points de départ raisonnables :
   *   – no-weather    : vecteur de features compact → modèle plus léger,
   *   – with-weather  : beaucoup de lags météo Wo/Wd → davantage d’arbres,
   *   – article-weather : sous-ensemble météo ciblé → un peu plus d’arbres / profondeur.
   *
   * Elles peuvent être ajustées en fonction des temps de calcul et des résultats observés.
   */
  private def rfHyperParamsFor(mode: FeatureSetMode): RfHyperParams = mode match {
    case NoWeather =>
      RfHyperParams(
        numTrees            = 80,
        maxDepth            = 12,
        minInstancesPerNode = 100,
        subsamplingRate     = 0.7
      )

    case WithWeather =>
      RfHyperParams(
        numTrees            = 150,
        maxDepth            = 16,
        minInstancesPerNode = 50,
        subsamplingRate     = 0.7
      )

    case ArticleWeather =>
      RfHyperParams(
        numTrees            = 200,
        maxDepth            = 18,
        minInstancesPerNode = 30,
        subsamplingRate     = 0.8
      )
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
      //   – "article-weather"   → seulement les variables T, H, Wd, Ws, P, S, V, Di,
      //                           agrégées en indices Wo/Wd synthétiques.
      // ------------------------------------------------------------------
      val prepared = prepareBaseDataset(joined)

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
   *  - colonnes de vol (date, horaires, aéroports),
   *  - colonnes de retard (WEATHER_DELAY, NAS_DELAY, indicateurs),
   *  - lags météo à l'origine et à destination Wo/Wd (si le mode inclut la météo),
   *  - variables temporelles simples (heure de départ, jour de la semaine),
   *  - traitement des NULL et NaN numériques.
   *
   * Lorsque le mode courant est "no-weather", cette méthode construit un
   * jeu de features "baseline" sans météo, comme dans les expériences
   * où l'article compare avec / sans variables météo.
   *
   * Lorsque normalizedFeatureSetId = "article-weather", seules les
   * variables météo correspondant à T, H, Ws, Wd, P, S et Di sont
   * conservées via des indices Wo/Wd synthétiques (cf. section 2.3).
   */
  private def prepareBaseDataset(
                                  joined: DataFrame
                                ): DataFrame = {

    val sc = spark.sparkContext
    sc.setJobDescription("TrainRF: preparation des colonnes de base")

    // Fenêtre maximale de lags météo : capée à 7 comme dans le code d'origine.
    val effectiveLags = math.min(lags, 7)

    // Colonnes de vol indispensables au label et aux features.
    val baseColNames = Seq(
      "FL_DATE",
      "CRS_DEP_TIMESTAMP",
      "CRS_ELAPSED_TIME",
      "ORIGIN_AIRPORT_ID",
      "DEST_AIRPORT_ID",
      "ARR_DELAY_NEW"
    ).filter(joined.columns.contains)

    val baseCols = baseColNames.map(col)

    // Colonnes de retard nécessaires à la définition des jeux D1..D4
    // (article section 4.2, "Bad-weather delays detection").
    val delayColNames = Seq(
      "WEATHER_DELAY",
      "NAS_DELAY",
      "HAS_WEATHER_DELAY",
      "HAS_NAS_DELAY"
    ).filter(joined.columns.contains)

    val delayCols = delayColNames.map(col)

    val hasWeatherArrays =
      joined.columns.contains("weather_origin") &&
        joined.columns.contains("weather_dest")

    // Construction du bloc (vol + éventuelle météo), avant ajout des variables temporelles.
    val withWeather: DataFrame =
      if (!mode.includeWeather || !hasWeatherArrays) {
        if (mode.includeWeather && !hasWeatherArrays) {
          log.warn(
            "[Train] mode.includeWeather=true mais colonnes weather_origin/weather_dest absentes : " +
              "aucune feature meteo exploitable, retour au dataset sans meteo."
          )
        }
        joined.select((baseCols ++ delayCols): _*)
      } else {
        mode match {
          case ArticleWeather =>
            // Mode article-weather : indices Wo_*/Wd_* synthétiques alignés sur T, H, Ws, Wd, P, S, Di.
            buildArticleWeatherDataset(joined, baseCols, delayCols, effectiveLags)

          case _ =>
            // Mode with-weather : comportement "full" historique, tous les lags Wo/Wd détaillés.
            buildFullWeatherLagDataset(joined, baseCols, delayCols, effectiveLags)
        }
      }

    val baseWithTime =
      withWeather
        // Variables temporelles simples (heure de départ, jour de la semaine).
        .withColumn("dep_hour", hour(col("CRS_DEP_TIMESTAMP")))
        .withColumn("dep_dow",  dayofweek(col("FL_DATE")))

    // Les lignes sans ARR_DELAY_NEW ne permettent pas de construire un label binaire.
    val cleaned = baseWithTime.filter(col("ARR_DELAY_NEW").isNotNull)

    // Remplacement des NULL numeriques.
    //
    // - En mode "no-weather" ou "with-weather", on remplit toutes les colonnes
    //   numeriques a 0.0 (comportement historique).
    // - En mode "article-weather", on evite d'ecraser les indices Wo_*/Wd_ :
    //   les flags *_missing portent l'information de presence/absence de meteo.
    val allNumericCols = cleaned.schema.fields.collect {
      case f if f.dataType.isInstanceOf[NumericType] => f.name
    }

    val cleanedNoNulls: DataFrame =
      if (allNumericCols.isEmpty) {
        cleaned
      } else if (normalizedFeatureSetId == "article-weather") {
        // On ne remplit pas les colonnes Wo_*/Wd_ ici, uniquement les autres.
        val weatherFeatureNames = allNumericCols.filter { name =>
          name.startsWith("Wo_") || name.startsWith("Wd_")
        }
        val nonWeatherNumericCols =
          allNumericCols.filterNot(weatherFeatureNames.toSet)

        if (nonWeatherNumericCols.nonEmpty)
          cleaned.na.fill(0.0, nonWeatherNumericCols)
        else
          cleaned
      } else {
        // with-weather / no-weather : remplissage 0.0 de toutes les colonnes numeriques.
        cleaned.na.fill(0.0, allNumericCols)
      }

    // Nettoyage des NaN / Inf dans les colonnes flottantes.
    //
    // Le RandomForest de Spark n'accepte ni NaN ni Inf dans le vecteur de features.
    // Or certaines colonnes meteo contiennent des NaN (valeurs manquantes encodees
    // en NaN dans les CSV d'origine).
    //
    // On remplace donc, pour toutes les colonnes Double/Float :
    //   - NaN            -> 0.0
    //   - +Infinity/-Inf -> 0.0
    //
    // Ce traitement est applique a tous les modes de features
    // (with-weather, no-weather, article-weather).
    val floatDoubleCols: Array[String] =
      cleanedNoNulls.schema.fields.collect {
        case f if f.dataType == DoubleType || f.dataType == FloatType => f.name
      }

    val cleanedFinal =
      floatDoubleCols.foldLeft(cleanedNoNulls) { (df, name) =>
        df.withColumn(
          name,
          when(
            isnan(col(name)) ||
              col(name) === lit(Double.PositiveInfinity) ||
              col(name) === lit(Double.NegativeInfinity),
            lit(0.0)
          ).otherwise(col(name))
        )
      }

    val nClean = cleanedFinal.count()
    log.info(
      s"[Train] Dataset apres preparation (featureSet=$normalizedFeatureSetId, includeWeather=${mode.includeWeather}) : " +
        s"$nClean lignes, ${cleanedFinal.columns.length} colonnes"
    )

    cleanedFinal
  }

  /**
   * Mode "with-weather" : déploie toutes les séries Wo/Wd en colonnes de lag.
   *
   * On garde le comportement d’origine :
   *   – tous les champs numériques des structs météo (hors w_ts)
   *   – lags 0..effectiveLags-1 à l’origine et à destination,
   *   – alias orig_<champ>_lagK et dest_<champ>_lagK.
   *
   * Le traitement des NULL (0.0 vs NaN) est géré plus haut dans prepareBaseDataset.
   */
  private def buildFullWeatherLagDataset(
                                          joined: DataFrame,
                                          baseCols: Seq[Column],
                                          delayCols: Seq[Column],
                                          effectiveLags: Int
                                        ): DataFrame = {

    val originStruct =
      joined.schema("weather_origin").dataType
        .asInstanceOf[ArrayType]
        .elementType
        .asInstanceOf[StructType]

    val numericFields: Seq[String] =
      originStruct.fields
        .filter(f => f.dataType.isInstanceOf[NumericType])
        .map(_.name)
        .filterNot(_ == "w_ts")

    log.info(
      s"[Train] Champs météo numériques Wo/Wd (with-weather) : ${numericFields.sorted.mkString(", ")}"
    )

    def lagCols(prefix: String, arrayCol: String): Seq[Column] =
      (0 until effectiveLags).flatMap { k =>
        numericFields.map { f =>
          col(arrayCol).getItem(k).getField(f).alias(s"${prefix}_${f}_lag$k")
        }
      }

    val weatherCols =
      lagCols("orig", "weather_origin") ++ lagCols("dest", "weather_dest")

    joined.select((baseCols ++ delayCols ++ weatherCols): _*)
  }

  /**
   * Mode "article-weather" aligné sur la section 2.3 de l’article (Belcastro et al., TIST 2016).
   *
   * Ce mode expose explicitement, pour chaque vol, les variables météo d’origine (Wo_*)
   * et de destination (Wd_*) suivantes, pour les 7 heures précédant le vol (lags 0 à 6) :
   * – T : température (Wo_T_lag0, ..., Wo_T_lag6)
   * – H : humidité relative
   * – Ws : vitesse du vent
   * – Wd : direction du vent
   * – P : pression au niveau de la mer
   * – S : nébulosité maximale (couche la plus dense)
   * – V : distance de visibilité
   * – Di : indice de sévérité météo (score cumulé sur RA, TS, FG, BR, FZ, SN, SH, DZ)
   *
   * Contrairement à la version précédente qui faisait une moyenne glissante,
   * on expose ici chaque observation horaire distinctement, comme suggéré dans l’article
   * (mentions explicites de tests sur fenêtres 0 à 11 heures).
   *
   * Pour chaque feature, on ajoute :
   * – un flag *_missing pour signaler l’absence de données (NULL),
   * – une valeur imputée par défaut pour éviter les NULL dans le vecteur de features.
   */
  private def buildArticleWeatherDataset(
                                          base: DataFrame,
                                          baseCols: Seq[Column],
                                          delayCols: Seq[Column],
                                          effectiveLags: Int
                                        ): DataFrame = {
    val maxLag = math.min(effectiveLags - 1, 6) // Cap à lag6

    var df = base.select((baseCols ++ delayCols :+ col("weather_origin") :+ col("weather_dest")): _*)

    // Boucle sur les 7 lags horaires (0 à 6)
    for (lag <- 0 to maxLag) {
      val origin = col("weather_origin").getItem(lag)
      val dest   = col("weather_dest").getItem(lag)

      df = df
        // Origine : T, H, Ws, Wd, P
        .withColumn(s"Wo_T_lag$lag", origin.getField("DryBulbFarenheit"))
        .withColumn(s"Wo_H_lag$lag", origin.getField("RelativeHumidity"))
        .withColumn(s"Wo_Ws_lag$lag", origin.getField("WindSpeed"))
        .withColumn(s"Wo_Wd_lag$lag", origin.getField("WindDirection"))
        .withColumn(s"Wo_P_lag$lag", origin.getField("SeaLevelPressure"))
        // Couverture nuageuse maximale (proxy via sky_num_layers)
        .withColumn(s"Wo_S_lag$lag", origin.getField("sky_num_layers"))
        // Visibilité (champ prétraité dans WeatherRawToClean)
        .withColumn(s"Wo_visibility_lag$lag", origin.getField("Visibility"))
        // Di = score cumulé des phéno météo horaires
        .withColumn(s"Wo_Di_lag$lag", sumWeatherScores(origin))

        // Destination
        .withColumn(s"Wd_T_lag$lag", dest.getField("DryBulbFarenheit"))
        .withColumn(s"Wd_H_lag$lag", dest.getField("RelativeHumidity"))
        .withColumn(s"Wd_Ws_lag$lag", dest.getField("WindSpeed"))
        .withColumn(s"Wd_Wd_lag$lag", dest.getField("WindDirection"))
        .withColumn(s"Wd_P_lag$lag", dest.getField("SeaLevelPressure"))
        .withColumn(s"Wd_S_lag$lag", dest.getField("sky_num_layers"))
        .withColumn(s"Wd_visibility_lag$lag", dest.getField("Visibility"))
        .withColumn(s"Wd_Di_lag$lag", sumWeatherScores(dest))
    }

    // Ajout des flags *_missing et remplacement des NULLs par valeur de repli (e.g. 0.0)
    val vars     = Seq("T", "H", "Ws", "Wd", "P", "S", "visibility", "Di")
    val features = vars.flatMap(v => (0 to maxLag).flatMap(lag => Seq(s"Wo_${v}_lag$lag", s"Wd_${v}_lag$lag")))

    features.foldLeft(df) { (acc, name) =>
      acc.withColumn(s"${name}_missing", when(col(name).isNull, 1.0).otherwise(0.0))
        .withColumn(name, coalesce(col(name), lit(0.0)))
    }
  }

  /**
   * Construit un score Di horaire (cf. article section 2.3) en sommant les scores
   * météorologiques associés aux phénomènes significatifs :
   * pluie (RA), orages (TS), brouillard (FG/BR), verglas (FZ), neige (SN), etc.
   *
   * L’idée est de produire un score agrégé d’intensité météo par observation horaire.
   * Si tous les champs sont nuls ou absents, on renvoie NULL.
   */
  private def sumWeatherScores(row: Column): Column = {
    val fields = Seq("wt_score_RA", "wt_score_TS", "wt_score_FG", "wt_score_BR",
      "wt_score_FZ", "wt_score_SN", "wt_score_SH", "wt_score_DZ")
    fields.map(row.getField).reduce(_ + _)
  }

  // ---------------------------------------------------------------------------
  // Sélection des vols positifs pour D1..D4 (section 4.2 de l’article TIST)
  // ---------------------------------------------------------------------------

  /**
   * Détermine si un vol est classé comme "positif" (retard significatif dû à la météo)
   * selon la définition du jeu D1, D2, D3 ou D4, utilisée dans les expériences de l’article.
   *
   * L'article (Belcastro et al., TIST 2016, section 4.2, p.5-6) distingue soigneusement :
   *   - les retards "vraiment" liés à la météo,
   *   - la part de NAS_DELAY causée par des événements météo (estimée à 58.3 % en 2013),
   *   - les autres causes (Late Aircraft, Carrier Delay, etc.).
   *
   * Cette fonction reproduit fidèlement ces définitions, avec le facteur correcteur
   * appliqué à NAS_DELAY pour refléter la part imputable à la météo (nasWeatherFactor).
   *
   * @param datasetId  nom du jeu ciblé (D1, D2, D3, D4 ou ALL)
   * @param threshold  seuil en minutes pour considérer un vol comme "retardé" (ex. 60)
   */
  private def positiveFilterForDataset(datasetId: String,
                                       threshold: Double): Column = {

    // Valeurs de base
    val delayMinutes   = coalesce(col("ARR_DELAY_NEW"), lit(0.0))
    val weatherMinutes = coalesce(col("WEATHER_DELAY"), lit(0.0))
    val nasMinutes     = coalesce(col("NAS_DELAY"), lit(0.0))

    // Flags (HAS_NAS_DELAY, HAS_WEATHER_DELAY) parfois utiles pour robustesse
    val hasWeatherFlag = coalesce(col("HAS_WEATHER_DELAY").cast("boolean"), lit(false))
    val hasNasFlag     = coalesce(col("HAS_NAS_DELAY").cast("boolean"), lit(false))

    // Retard total jugé significatif (ex. ≥ 60 min)
    val isDelayed = delayMinutes >= lit(threshold)

    // ⚠️ Correction apportée au NAS_DELAY pour refléter uniquement la part météo
    // L’article dit que 58.3 % des retards NAS sont liés à la météo (page 6, tableau V)
    // Cette part est fixe dans les données FAA 2013, utilisée comme référence pour 2012 aussi.
    val nasWeatherFactor = 0.583
    val nasWeatherMinutes = nasMinutes * nasWeatherFactor

    // Somme des minutes météo : WEATHER_DELAY + part NAS liée à la météo
    val weatherTotal = weatherMinutes + nasWeatherMinutes

    // Heuristique supplémentaire utilisée dans l’article : "hasWeatherOrNasFlag"
    val hasWeatherOrNasFlag = hasWeatherFlag || hasNasFlag

    // Estimation des minutes dues à d'autres causes
    val otherCauseMinutes = delayMinutes - weatherTotal

    // Cas spécifiques pour chaque dataset
    datasetId.toUpperCase match {

      // ---------------------------------------------------------------------
      // D1 – "Retards quasi entièrement dus à la météo ou NAS lié à météo"
      // Article : "flights for which delay is almost completely caused by weather or NAS related to weather"
      // ---------------------------------------------------------------------
      case "D1" =>
        isDelayed &&
          (weatherTotal > 0.0 || hasWeatherOrNasFlag) &&
          (otherCauseMinutes <= 0.5)

      // ---------------------------------------------------------------------
      // D2 – "Retards causés par météo OU NAS_DELAY (lié à météo) ≥ seuil"
      // Article : "NAS delay greater than or equal to threshold" (avec correction météo)
      // ---------------------------------------------------------------------
      case "D2" =>
        isDelayed && (
          weatherMinutes > 0.0 ||
            nasWeatherMinutes >= threshold
          )

      // ---------------------------------------------------------------------
      // D3 – "Retards où météo ou NAS lié à météo interviennent"
      // Article : "flights delayed by weather or by NAS related to weather"
      // ---------------------------------------------------------------------
      case "D3" =>
        isDelayed && (
          weatherTotal > 0.0 || hasWeatherOrNasFlag
          )

      // ---------------------------------------------------------------------
      // D4 – Tous les vols retardés, sans distinction de cause
      // Article : "includes all delayed flights with no filtering on delay causes"
      // ---------------------------------------------------------------------
      case "D4" =>
        isDelayed

      // ---------------------------------------------------------------------
      // ALL – Comportement proche de D3, équivalent dans notre implémentation
      // ---------------------------------------------------------------------
      case "ALL" | "D_ALL" =>
        isDelayed && (
          weatherTotal > 0.0 || hasWeatherOrNasFlag
          )

      // Sécurité : dataset non reconnu
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

    val isOnTime     = col("ARR_DELAY_NEW") < threshold
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

    // On caste explicitement toutes les features en double.
    // Les NaN et Inf éventuels ont été neutralisés plus haut dans prepareBaseDataset.
    val casted = featureCols.foldLeft(trainDF) { (df, c) =>
      df.withColumn(c, col(c).cast("double"))
    }

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
      .setHandleInvalid("keep")

    // Hyperparamètres inspirés de l’article (section 4.3), adaptés par mode :
    //   – vecteur "compact" no-weather → modèle plus simple,
    //   – vecteur riche en lags météo (with/article-weather) → davantage d’arbres / profondeur.
    val hp = rfHyperParamsFor(mode)

    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(hp.numTrees)
      .setMaxDepth(hp.maxDepth)
      .setFeatureSubsetStrategy(hp.featureSubsetStrategy)
      .setSubsamplingRate(hp.subsamplingRate)
      .setMinInstancesPerNode(hp.minInstancesPerNode)
      .setSeed(hp.seed)

    val pipeline = new Pipeline().setStages(Array(assembler, rf))

    log.info(
      s"[Train] Démarrage RF (featureSet=$normalizedFeatureSetId) : " +
        s"numTrees=${hp.numTrees}, maxDepth=${hp.maxDepth}, " +
        s"subsamplingRate=${hp.subsamplingRate}, minInstancesPerNode=${hp.minInstancesPerNode}, " +
        s"featureSubsetStrategy=${hp.featureSubsetStrategy}"
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
