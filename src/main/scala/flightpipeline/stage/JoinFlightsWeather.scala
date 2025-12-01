package flightpipeline.stage

import flightpipeline.util.{ProgressListener, UiLogger}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

/**
 * Jointure vols / météo inspirée de Belcastro et al., TIST 2010
 * "Using Scalable Data Mining for Predicting Flight Delays".
 *
 * Objectif principal (cf. article, sections 2.2–2.3) :
 *   - pour chaque vol Fs = <Ao, Ad, tsd, tsa>,
 *   - construire deux tableaux :
 *       Wo = <O(Ao, tsd), O(Ao, tsd − 1h), …>
 *       Wd = <D(Ad, tsa), D(Ad, tsa − 1h), …>
 *   - ces tableaux contiennent les mesures météo observées aux
 *     aéroports d’origine et de destination, dans une fenêtre
 *     temporelle finie avant le départ / l’arrivée.
 *
 * Dans cette implémentation :
 *   - Wo et Wd sont stockés dans deux colonnes
 *       weather_origin, weather_dest
 *     de type array<struct<w_ts, … variables météo …>>,
 *   - les éléments sont triés du plus récent au plus ancien,
 *     ce qui correspond à l’ordre décrit dans l’article,
 *   - la profondeur temporelle (fenêtre et nombre de lags) est
 *     paramétrable via `windowHours` et `lags`.
 *
 * Sortie principale : une table Delta `join_intermediate` qui sert
 * de base à la construction des ensembles D1..D4 et à l’entraînement.
 */
final class JoinFlightsWeather(
                                spark: SparkSession,
                                flightCleanPath: String,
                                weatherCleanPath: String,
                                airportTimezonePath: String,
                                outIntermediate: String,
                                outFlat: String,
                                windowHours: Int,
                                lags: Int,
                                sampleMonth: Option[String]
                              )
{
  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Sélection des colonnes météo utiles.
   *
   * Hypothèses sur `weatherRaw` :
   *   - contient une colonne temporelle déjà renommée en `w_ts`
   *   - contient une colonne `AirportId` (clé aéroport).
   *
   * Cf. article, section 3 ("Data Understanding") :
   *   les auteurs retiennent surtout des variables thermiques,
   *   de pression, de vent, de visibilité, de précipitation et des
   *   indicateurs de phénomènes (RA, SN, FG, TS, …).
   *   La liste ci‑dessous suit cette logique en conservant les
   *   variables dérivées construites dans `WeatherRawToClean`.
   *
   * Retour :
   *   - DataFrame avec schéma :
   *       apt_id : ID aéroport
   *       w_ts   : timestamp météo
   *       …      : variables météo numériques / booléennes
   *   - liste des colonnes de features météo (hors apt_id et w_ts)
   */
  private def selectWeatherColumns(weatherRaw: DataFrame): (DataFrame, Seq[String]) = {
    // Élimination d’un éventuel struct intermédiaire `weather_scores`
    // pour éviter les ambigüités de nom de colonnes.
    val base0 =
      if (weatherRaw.columns.contains("weather_scores")) weatherRaw.drop("weather_scores")
      else weatherRaw

    // Colonnes météo retenues, en cohérence avec les familles de
    // variables utilisées dans l’article.
    val keep = Seq(
      "w_ts", // timestamp météo (pivot pour l’ordre Wo / Wd)

      // Température / humidité / pression
      "DryBulbFarenheit", "WetBulbFarenheit", "DewPointFarenheit",
      "RelativeHumidity",
      "SeaLevelPressure", "StationPressure", "Altimeter",

      // Vent / visibilité / précipitations / dynamique de pression
      "WindSpeed", "WindDirection", "ValueForWindCharacter",
      "Visibility", "HourlyPrecip",
      "PressureTendency", "PressureChange",

      // Géométrie des couches nuageuses. L’article reste assez
      // succinct sur ce point ; ces indicateurs offrent une
      // description plus riche de la couverture nuageuse.
      "sky_num_layers", "sky_min_altitude", "sky_max_altitude", "sky_mean_altitude",
      "sky_has_CB", "sky_has_TCU",
      "sky_has_OVC", "sky_has_BKN", "sky_has_SCT",
      "sky_has_FEW", "sky_has_VV",

      // Scores pondérés par type de phénomène météo.
      // L’article insiste en particulier sur la pluie (RA),
      // les orages (TS), le brouillard (FG), la neige (SN),
      // le verglas (FZ) et les averses (SH).
      "wt_score_RA", "wt_score_TS", "wt_score_FG", "wt_score_BR",
      "wt_score_FZ", "wt_score_SN", "wt_score_SH", "wt_score_DZ"
    ).filter(base0.columns.contains)

    val selectedCols: Seq[Column] =
      Seq(col("AirportId").alias("apt_id")) ++ keep.map(col)

    val selected = base0.select(selectedCols: _*)

    val featureCols =
      selected.columns.filterNot(c => c == "apt_id" || c == "w_ts")

    (selected, featureCols)
  }

  /**
   * Point d’entrée principal de l’étape de jointure.
   *
   * Étapes principales :
   *   1. Chargement des vols propres (flight_clean) et calcul du
   *      timestamp d’arrivée théorique.
   *   2. Détermination des bornes temporelles nécessaires pour la météo.
   *   3. Chargement de la météo (weather_clean) filtrée sur cette fenêtre,
   *      renommage du timestamp en `w_ts`, sélection des variables utiles.
   *   4. Jointure avec la météo d’origine (Wo), puis de destination (Wd),
   *      en respectant la fenêtre temporelle décrite dans l’article.
   *   5. Écriture de la table Delta `join_intermediate`.
   */
  def run(): DataFrame = {
    log.info(
      s"[Join] flights=$flightCleanPath ; weather=$weatherCleanPath ; " +
        s"hours=$windowHours ; lags=$lags"
    )

    // Réglages d’exécution Spark : AQE, coalescing dynamique, skew join.
    // Ces optimisations ne sont pas détaillées dans l’article mais
    // améliorent la robustesse de l’implémentation sur de gros volumes.
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.files.maxRecordsPerFile", "5000000")

    val sc = spark.sparkContext
    sc.setJobGroup("JOIN-FLW", s"Join flights-weather (lags=$lags, window=$windowHours)")
    sc.setJobDescription("Initialisation de la jointure vols‑météo")

    val uiUrl = UiLogger.logUiUrl(spark)
    println(s"[ui] ouverture de l’UI Spark possible avec : wslview $uiUrl")
    val _listener = ProgressListener.register(sc, 2000L)

    var wSelAllPersisted: Option[DataFrame] = None
    var flightsPersisted: Option[DataFrame] = None
    var airportTzPersisted: Option[DataFrame] = None

    // Par symétrie avec l’article, l’algorithme s’exprime plus clairement
    // avec un nombre de lags ≥ 1.
    val effectiveLags: Int = math.max(1, lags)

    try {
      // ------------------------------------------------------------------
      // 0) Référentiel AirportId -> TimeZone (Delta)
      //
      // Construit une seule fois dans WeatherRawToClean, on le réutilise
      // ici pour convertir vols et météo en UTC avant d’appliquer la
      // fenêtre de 12h (cf. article, sections 2.2–2.3).
      // ------------------------------------------------------------------
      sc.setJobDescription("Chargement du référentiel airport_timezone_clean")

      val airportTz = spark.read
        .format("delta")
        .load(airportTimezonePath)
        .select(
          col("AirportId").cast("int").as("AirportId"),
          col("TimeZone").cast("int").as("TimeZone")
        )
        .distinct()
        .persist(StorageLevel.MEMORY_AND_DISK)

      airportTzPersisted = Some(airportTz)

      // ------------------------------------------------------------------
      // 0bis) Bornes temporelles de la météo en UTC
      //
      // On calcule min/max des timestamps météo disponibles (w_ts_utc)
      // pour filtrer ensuite les vols qui sortent complètement de cette
      // période. Cela évite de faire passer des vols "hors météo" dans
      // les grosses jointures non-équi.
      // ------------------------------------------------------------------
      sc.setJobDescription("Calcul des bornes temporelles de la météo (UTC)")

      val weatherBoundsRow = spark.read
        .format("delta")
        .load(weatherCleanPath)
        .join(airportTz, Seq("AirportId"), "left")
        .withColumn(
          "w_ts_utc",
          expr("timestampadd(HOUR, -coalesce(TimeZone, 0), timestamp)")
        )
        .agg(
          min(col("w_ts_utc")).as("min_w"),
          max(col("w_ts_utc")).as("max_w")
        )
        .first()

      val minWeatherTsUtc = weatherBoundsRow.getAs[java.sql.Timestamp]("min_w")
      val maxWeatherTsUtc = weatherBoundsRow.getAs[java.sql.Timestamp]("max_w")

      log.info(s"[Join] Fenêtre météo disponible (UTC) = [$minWeatherTsUtc .. $maxWeatherTsUtc]")


      // ------------------------------------------------------------------
      // 1) Vols : colonnes nécessaires et timestamps
      //
      // Référence article : définition de Fs = <Ao, Ad, tsd, tsa>
      //   - Ao / Ad : ORIGIN_AIRPORT_ID, DEST_AIRPORT_ID
      //   - tsd     : CRS_DEP_TIMESTAMP
      //   - tsa     : CRS_ARR_TIMESTAMP (reconstruit)
      // ------------------------------------------------------------------
      sc.setJobDescription("Chargement et préparation des vols (flight_clean)")

      val flightsSrc = spark.read.format("delta").load(flightCleanPath)

      // Colonnes strictement nécessaires pour :
      //  - la jointure avec la météo,
      //  - la définition du label "retard ≥ seuil",
      //  - la reconstruction des ensembles D1–D4 (section 4.2) qui
      //    distinguent les retards « météo » des autres causes.
      val mustHave = Seq(
        "FL_DATE",
        "CRS_DEP_TIMESTAMP",
        "CRS_ELAPSED_TIME",
        "ORIGIN_AIRPORT_ID",
        "DEST_AIRPORT_ID",
        "ARR_DELAY_NEW",
        "WEATHER_DELAY",
        "NAS_DELAY",
        "HAS_WEATHER_DELAY",
        "HAS_NAS_DELAY"
      )

      val optional = Seq(
        "CANCELLED",
        "DIVERTED",
        "OP_UNIQUE_CARRIER",
        "OP_CARRIER",
        "OP_CARRIER_FL_NUM",
        "OP_CARRIER_AIRLINE_ID"
      )

      val present  = flightsSrc.columns.toSet
      val keepCols = mustHave ++ optional.filter(present.contains)

      // Filtrage éventuel sur un mois YYYYMM
      val flightsFiltered = sampleMonth match {
        case Some(yyyymm) =>
          log.info(s"[Join] Filtrage des vols sur sampleMonth=$yyyymm (FL_DATE en yyyyMM)")
          val df = flightsSrc
            .filter(date_format(col("FL_DATE"), "yyyyMM") === lit(yyyymm))

          val cnt = df.count()
          val db  = df
            .select(
              min(col("FL_DATE")).as("min_date"),
              max(col("FL_DATE")).as("max_date")
            )
            .first()

          log.info(
            s"[Join] Après filtre mois $yyyymm : rows=$cnt, FL_DATE=[${db.getAs[java.sql.Date]("min_date")} .. ${db.getAs[java.sql.Date]("max_date")}]"
          )
          df

        case None =>
          flightsSrc
      }

      val flightsLocal = flightsFiltered
        .select(keepCols.map(col): _*)

      // Conversion des horaires planifiés en UTC à partir du fuseau de l'aéroport d'origine.
      // Idée :
      //   - CRS_DEP_TIMESTAMP est en heure locale origine (comme dans l’article pour tsd),
      //   - on le passe en UTC via TimeZone,
      //   - puis on ajoute CRS_ELAPSED_TIME pour obtenir l’instant d’arrivée en UTC.
      // Conversion en UTC côté vols
      val flightsUtc = flightsLocal
        .join(
          airportTz.withColumnRenamed("AirportId", "ORIGIN_AIRPORT_ID"),
          Seq("ORIGIN_AIRPORT_ID"),
          "left"
        )
        .withColumnRenamed("TimeZone", "TZ_ORIGIN")
        .withColumn(
          "CRS_DEP_TIMESTAMP",
          expr("timestampadd(HOUR, -coalesce(TZ_ORIGIN, 0), CRS_DEP_TIMESTAMP)")
        )
        .withColumn(
          "CRS_ARR_TIMESTAMP",
          expr("timestampadd(SECOND, coalesce(CRS_ELAPSED_TIME, 0) * 60, CRS_DEP_TIMESTAMP)")
        )
        .drop("TZ_ORIGIN")

      // Filtrage des vols à la fenêtre météo disponible
      // On ne garde que les vols dont la fenêtre [dep - H, arr] est
      // compatible avec [minWeatherTsUtc, maxWeatherTsUtc + 1h].
      val minAllowedDep = new java.sql.Timestamp(
        minWeatherTsUtc.getTime + windowHours.toLong * 3600L * 1000L
      )
      val maxAllowedArr = new java.sql.Timestamp(
        maxWeatherTsUtc.getTime + 1L * 3600L * 1000L
      )

      val flights = flightsUtc
        .filter(col("CRS_DEP_TIMESTAMP") >= lit(minAllowedDep))
        .filter(col("CRS_ARR_TIMESTAMP") <= lit(maxAllowedArr))
        .withColumn("flight_id", monotonically_increasing_id())
        .repartition(col("ORIGIN_AIRPORT_ID"))
        .persist(StorageLevel.MEMORY_AND_DISK)

      flightsPersisted = Some(flights)

      // Bornes temporelles utiles pour la météo (sur les vols déjà filtrés)
      sc.setJobDescription("Calcul des bornes temporelles pour le filtrage météo")

      val bounds = flights
        .agg(
          min(col("CRS_DEP_TIMESTAMP")).as("min_dep"),
          max(col("CRS_ARR_TIMESTAMP")).as("max_arr")
        )
        .first()

      val minDep = bounds.getAs[java.sql.Timestamp]("min_dep")
      val maxArr = bounds.getAs[java.sql.Timestamp]("max_arr")

      val minCut = new java.sql.Timestamp(
        minDep.getTime - windowHours.toLong * 3600L * 1000L
      )
      val maxCut = new java.sql.Timestamp(
        maxArr.getTime + 1L * 3600L * 1000L
      )

      log.info(s"[Join] Fenêtre temporelle météo utilisée = [$minCut .. $maxCut]")
      // ------------------------------------------------------------------
      // 2) Météo : filtrage temporel et sélection de features
      //
      // Référence article : "Joint Table" (section 2.3) et Algorithme 1.
      // La logique de filtrage [ts − windowHours, ts] est exprimée
      // directement en timestamps plutôt qu’en (date, heure).
      // ------------------------------------------------------------------
      sc.setJobDescription("Chargement de la météo filtrée et sélection des variables")

      // Lecture de la météo "clean" + jointure sur le fuseau de chaque aéroport
      val weatherRaw = spark.read.format("delta").load(weatherCleanPath)
        .join(airportTz, Seq("AirportId"), "left")
        // Conversion du timestamp météo local -> UTC
        .withColumn(
          "w_ts_utc",
          expr("timestampadd(HOUR, -coalesce(TimeZone, 0), timestamp)")
        )

      // Filtrage sur la fenêtre temporelle utile en UTC, comme pour les vols
      val weatherBase = weatherRaw
        .where(col("w_ts_utc").between(lit(minCut), lit(maxCut)))
        .repartition(col("AirportId"))

      // Renommage du timestamp UTC en `w_ts` pour Wo / Wd
      val weatherWithTs = weatherBase.withColumnRenamed("w_ts_utc", "w_ts")

      val (wSelAll0, featureCols) = selectWeatherColumns(weatherWithTs)
      val wSelAll = wSelAll0.persist(StorageLevel.MEMORY_AND_DISK)
      wSelAllPersisted = Some(wSelAll)

      log.info(
        s"[Join] Nombre de variables météo retenues (hors w_ts) : ${featureCols.size} ; " +
          s"lags demandés = $lags, lags effectifs = $effectiveLags"
      )

      // Matérialisation des caches pour fixer le plan d’exécution.
      sc.setJobDescription("Matérialisation des caches vols / météo")
      flights.count()
      wSelAll.count()

      // ------------------------------------------------------------------
      // 3) Jointure avec la météo à l’origine (Wo)
      //
      // Référence : Wo = <O(Ao, tsd), O(Ao, tsd−1h), …>.
      // Pour chaque vol :
      //   - recherche des observations météo de l’aéroport d’origine
      //     dans [CRS_DEP_TIMESTAMP − windowHours, CRS_DEP_TIMESTAMP],
      //   - tri par date décroissante,
      //   - limitation aux `effectiveLags` mesures les plus récentes.
      // ------------------------------------------------------------------
      sc.setJobDescription("Jointure avec la météo ORIGINE (construction de Wo)")

      val wOrig = wSelAll
        .withColumnRenamed("apt_id", "orig_apt")
        .withColumnRenamed("w_ts",   "orig_w_ts")

      val joinOrig = flights.join(
        wOrig,
        flights("ORIGIN_AIRPORT_ID") === col("orig_apt") &&
          col("orig_w_ts") <= flights("CRS_DEP_TIMESTAMP") &&
          col("orig_w_ts") >= (flights("CRS_DEP_TIMESTAMP") - expr(s"INTERVAL $windowHours HOURS")),
        "left"
      )

      val wDescOrig =
        Window.partitionBy(col("flight_id")).orderBy(col("orig_w_ts").desc)

      val origRanked = joinOrig
        .filter(col("orig_w_ts").isNotNull)
        .withColumn("rk", row_number().over(wDescOrig))
        .filter(col("rk") <= effectiveLags)
        .drop("rk")

      val originAggRaw = origRanked
        .groupBy(col("flight_id"))
        .agg(
          collect_list(
            struct(
              col("orig_w_ts").as("w_ts") +:
                featureCols.map(col): _*
            )
          ).as("weather_origin")
        )

      val originAgg = originAggRaw.withColumn(
        "weather_origin",
        expr(s"slice(reverse(sort_array(weather_origin, false)), 1, $effectiveLags)")
      )

      // ------------------------------------------------------------------
      // 4) Jointure avec la météo à destination (Wd)
      //
      // Référence : Wd = <D(Ad, tsa), D(Ad, tsa−1h), …>.
      // Construction symétrique de Wo, basée cette fois sur l’heure
      // d’arrivée planifiée.
      // ------------------------------------------------------------------
      sc.setJobDescription("Jointure avec la météo DESTINATION (construction de Wd)")

      val wDest = wSelAll
        .withColumnRenamed("apt_id", "dest_apt")
        .withColumnRenamed("w_ts",   "dest_w_ts")

      val flightsForDest = flights.repartition(col("DEST_AIRPORT_ID"))

      val joinDest = flightsForDest.join(
        wDest,
        flightsForDest("DEST_AIRPORT_ID") === col("dest_apt") &&
          col("dest_w_ts") <= flightsForDest("CRS_ARR_TIMESTAMP") &&
          col("dest_w_ts") >= (flightsForDest("CRS_ARR_TIMESTAMP") - expr(s"INTERVAL $windowHours HOURS")),
        "left"
      )

      val wDescDest =
        Window.partitionBy(col("flight_id")).orderBy(col("dest_w_ts").desc)

      val destRanked = joinDest
        .filter(col("dest_w_ts").isNotNull)
        .withColumn("rk", row_number().over(wDescDest))
        .filter(col("rk") <= effectiveLags)
        .drop("rk")

      val destAggRaw = destRanked
        .groupBy(col("flight_id"))
        .agg(
          collect_list(
            struct(
              col("dest_w_ts").as("w_ts") +:
                featureCols.map(col): _*
            )
          ).as("weather_dest")
        )

      val destAgg = destAggRaw.withColumn(
        "weather_dest",
        expr(s"slice(reverse(sort_array(weather_dest, false)), 1, $effectiveLags)")
      )

      // ------------------------------------------------------------------
      // 5) Fusion avec les colonnes de vols et écriture Delta
      //
      // Référence article : "Joint Table" JT (section 2.3).
      // La table jointe contient :
      //   - Fs (infos vol),
      //   - Wo et Wd (historique météo),
      //   - les colonnes de cause de retard nécessaires pour
      //     reconstruire les ensembles D1–D4 (section 4.2).
      // ------------------------------------------------------------------
      sc.setJobDescription("Fusion Wo/Wd avec les vols et écriture de join_intermediate")

      val baseColsFixed = Seq(
        "flight_id",
        "FL_DATE",
        "CRS_DEP_TIMESTAMP",
        "CRS_ELAPSED_TIME",
        "CRS_ARR_TIMESTAMP",
        "ORIGIN_AIRPORT_ID",
        "DEST_AIRPORT_ID",
        "ARR_DELAY_NEW",
        "WEATHER_DELAY",
        "NAS_DELAY",
        "HAS_WEATHER_DELAY",
        "HAS_NAS_DELAY"
      ) ++ Seq(
        "OP_UNIQUE_CARRIER",
        "OP_CARRIER",
        "OP_CARRIER_FL_NUM",
        "OP_CARRIER_AIRLINE_ID"
      ).filter(present.contains)

      val flightsBase = flights.select(baseColsFixed.map(col): _*)

      val joinedCore =
        flightsBase.join(originAgg, "flight_id").join(destAgg, "flight_id")

      val joined = joinedCore.select(
        (baseColsFixed.filter(_ != "flight_id").map(col) :+
          col("weather_origin") :+
          col("weather_dest")): _*
      )

      joined.write.format("delta").mode("overwrite").save(outIntermediate)
      log.info(s"[Join] Table intermédiaire (Delta) écrite dans $outIntermediate")

      // Bloc "flat + agrégats" désactivé pour l’instant.
      // Il permettrait de rapprocher encore davantage la construction
      // de features de la description de l’article (min, max, moyenne,
      // variation de certaines séries météo). Le volume produit en
      // local risquerait cependant d’être important.

      // Libération explicite de la mémoire
      wSelAllPersisted.foreach(_.unpersist())
      flightsPersisted.foreach(_.unpersist())
      airportTzPersisted.foreach(_.unpersist())

      // Retour de la table intermédiaire (utile pour les tests).
      spark.read.format("delta").load(outIntermediate)

    } finally {
      val sc2 = spark.sparkContext
      sc2.setJobDescription(null)
      sc2.clearJobGroup()
    }
  }
}
