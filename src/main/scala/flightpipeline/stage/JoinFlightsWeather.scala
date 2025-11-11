package flightpipeline.stage

import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{ArrayType, StructType, NumericType}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory
import flightpipeline.util.{ProgressListener, UiLogger}

final class JoinFlightsWeather(
                                spark: SparkSession,
                                flightCleanPath: String,
                                weatherCleanPath: String,
                                outIntermediate: String,
                                outFlat: String,              // conservé pour compatibilité d'API, non utilisé
                                windowHours: Int,
                                lags: Int
                              ) {

  private val log = LoggerFactory.getLogger(getClass)

  /** Sélectionne les colonnes météo utiles + renomme (AirportId -> apt_id, timestamp -> w_ts). */
  private def selectWeatherColumns(weatherRaw: DataFrame): (DataFrame, Seq[String]) = {
    val base0 =
      if (weatherRaw.columns.contains("weather_scores")) weatherRaw.drop("weather_scores")
      else weatherRaw

    val keep = Seq(
      "timestamp", // -> w_ts
      // thermiques & humidité & pression
      "DryBulbFarenheit","WetBulbFarenheit","DewPointFarenheit","RelativeHumidity",
      "SeaLevelPressure","StationPressure","Altimeter",
      // vent / visibilité / précip / dynamique pression
      "WindSpeed","WindDirection","ValueForWindCharacter","Visibility","HourlyPrecip","PressureTendency","PressureChange",
      // nuages & convection
      "sky_num_layers","sky_min_altitude","sky_max_altitude","sky_mean_altitude",
      "sky_has_CB","sky_has_TCU","sky_has_OVC","sky_has_BKN","sky_has_SCT","sky_has_FEW","sky_has_VV",
      // phénomènes (pondérés)
      "wt_score_RA","wt_score_TS","wt_score_FG","wt_score_BR","wt_score_FZ","wt_score_SN","wt_score_SH","wt_score_DZ"
    ).filter(base0.columns.contains)

    val selected = base0.select(
      col("AirportId").alias("apt_id") +:
        keep.map {
          case "timestamp" => col("timestamp").alias("w_ts")
          case other       => col(other)
        }: _*
    )

    val featureCols = selected.columns.filterNot(c => c == "apt_id" || c == "w_ts")
    (selected, featureCols)
  }

  def run(): DataFrame = {
    log.info(s"[Join] flights=$flightCleanPath ; weather=$weatherCleanPath ; hours=$windowHours ; lags=$lags")

    // (2) Réglages d'exécution : AQE & coalesce partitions & skew join
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

    // (3) Granularité fichiers (n'affecte pas les données)
    spark.conf.set("spark.sql.files.maxRecordsPerFile", "5000000")

    // progression / nommage des jobs (UI & logs)
    val sc = spark.sparkContext
    sc.setJobGroup("JOIN-FLW", s"Join flights-weather (lags=$lags, window=$windowHours)")
    sc.setJobDescription(s"Init join (lags=$lags)")
    val uiUrl = UiLogger.logUiUrl(spark)
    println(s"[ui] ouvrir dans Windows: wslview $uiUrl")
    val _listener = ProgressListener.register(sc, 2000L)

    var wSelAllPersisted: Option[DataFrame] = None
    var flightsPersisted: Option[DataFrame] = None

    try {
      // 1) VOL : colonnes nécessaires + timestamps
      sc.setJobDescription("Prepare flights (select, timestamps, repartition)")
      val flightsSrc = spark.read.format("delta").load(flightCleanPath)

      val mustHave = Seq(
        "FL_DATE","CRS_DEP_TIMESTAMP","CRS_ELAPSED_TIME",
        "ORIGIN_AIRPORT_ID","DEST_AIRPORT_ID","ARR_DELAY_NEW"
      )
      val optional = Seq("OP_UNIQUE_CARRIER","OP_CARRIER","OP_CARRIER_FL_NUM","OP_CARRIER_AIRLINE_ID")
      val present = flightsSrc.columns.toSet
      val keepCols = mustHave ++ optional.filter(present.contains)

      val flights = flightsSrc
        .select(keepCols.map(col): _*)
        .withColumn(
          "CRS_ARR_TIMESTAMP",
          (col("CRS_DEP_TIMESTAMP").cast("long") + (col("CRS_ELAPSED_TIME") * 60).cast("long")).cast("timestamp")
        )
        .withColumn("flight_id", monotonically_increasing_id())
        .repartition(col("ORIGIN_AIRPORT_ID"))
        .persist(StorageLevel.MEMORY_AND_DISK)

      flightsPersisted = Some(flights)
      log.info(s"[Join] flights columns kept = ${keepCols.mkString(",")}")

      // bornes temporelles pour pruner la météo
      sc.setJobDescription("Compute time bounds for weather pruning")
      val bounds = flights.agg(
        min(col("CRS_DEP_TIMESTAMP")).as("min_dep"),
        max(col("CRS_ARR_TIMESTAMP")).as("max_arr")
      ).first()

      val minNeeded = bounds.getAs[java.sql.Timestamp]("min_dep")
      val maxNeeded = bounds.getAs[java.sql.Timestamp]("max_arr")
      val minCut = new java.sql.Timestamp(minNeeded.getTime - windowHours.toLong * 3600 * 1000L)
      val maxCut = new java.sql.Timestamp(maxNeeded.getTime + 3600 * 1000L) // +1h de marge
      log.info(s"[Join] weather pruning window = [$minCut .. $maxCut]")

      // 2) METEO : prune temporel + whitelist
      sc.setJobDescription("Read & prune weather, select features")
      val weatherRaw = spark.read.format("delta").load(weatherCleanPath)
        .where(col("timestamp").between(lit(minCut), lit(maxCut)))
        .repartition(col("AirportId"))

      val (wSelAll0, featureCols0) = selectWeatherColumns(weatherRaw)
      val wSelAll = wSelAll0.persist(StorageLevel.MEMORY_AND_DISK)
      wSelAllPersisted = Some(wSelAll)
      val featureCols = featureCols0
      log.info(s"[Join] #features météo (hors w_ts): ${featureCols.size} ; lags=$lags")

      // matérialisation légère caches
      sc.setJobDescription("Materialize cached inputs")
      flights.count(); wSelAll.count()

      // 3) ORIGINE
      sc.setJobDescription("Join ORIGIN weather and limit to lags")
      val wOrig = wSelAll.withColumnRenamed("apt_id", "orig_apt")
        .withColumnRenamed("w_ts",  "orig_w_ts")

      val joinOrig = flights.join(
        wOrig,
        flights("ORIGIN_AIRPORT_ID") === col("orig_apt") &&
          col("orig_w_ts") <= flights("CRS_DEP_TIMESTAMP") &&
          col("orig_w_ts") >= (flights("CRS_DEP_TIMESTAMP") - expr(s"INTERVAL $windowHours HOURS")),
        "left"
      )

      val wDescOrig = Window.partitionBy(col("flight_id")).orderBy(col("orig_w_ts").desc)
      val origLimited =
        if (lags > 0) joinOrig.withColumn("rk", row_number().over(wDescOrig)).filter(col("rk") <= lags).drop("rk")
        else           joinOrig.withColumn("rk", row_number().over(wDescOrig)).filter(col("rk") === 1).drop("rk")

      sc.setJobDescription("Aggregate ORIGIN weather (collect_list)")
      val originAgg =
        if (lags > 0)
          origLimited.groupBy(col("flight_id"))
            .agg(collect_list(struct((col("orig_w_ts").as("w_ts") +: featureCols.map(col)):_*)).as("weather_origin"))
        else
          origLimited.select(
            col("flight_id"),
            struct((col("orig_w_ts").as("w_ts") +: featureCols.map(col)):_*).as("weather_origin_single")
          )

      // 4) DESTINATION
      sc.setJobDescription("Join DEST weather and limit to lags")
      val wDest = wSelAll.withColumnRenamed("apt_id", "dest_apt")
        .withColumnRenamed("w_ts",  "dest_w_ts")

      val flightsForDest = flights.repartition(col("DEST_AIRPORT_ID"))

      val joinDest = flightsForDest.join(
        wDest,
        flightsForDest("DEST_AIRPORT_ID") === col("dest_apt") &&
          col("dest_w_ts") <= flightsForDest("CRS_ARR_TIMESTAMP") &&
          col("dest_w_ts") >= (flightsForDest("CRS_ARR_TIMESTAMP") - expr(s"INTERVAL $windowHours HOURS")),
        "left"
      )

      val wDescDest = Window.partitionBy(col("flight_id")).orderBy(col("dest_w_ts").desc)
      val destLimited =
        if (lags > 0) joinDest.withColumn("rk", row_number().over(wDescDest)).filter(col("rk") <= lags).drop("rk")
        else           joinDest.withColumn("rk", row_number().over(wDescDest)).filter(col("rk") === 1).drop("rk")

      sc.setJobDescription("Aggregate DEST weather (collect_list)")
      val destAgg =
        if (lags > 0)
          destLimited.groupBy(col("flight_id"))
            .agg(collect_list(struct((col("dest_w_ts").as("w_ts") +: featureCols.map(col)):_* )).as("weather_dest"))
        else
          destLimited.select(
            col("flight_id"),
            struct((col("dest_w_ts").as("w_ts") +: featureCols.map(col)):_*).as("weather_dest_single")
          )

      // 5) Fusion sur flight_id puis ajout des colonnes vol
      sc.setJobDescription("Join ORIGIN/DEST aggregates with flights")

      val baseColsFixed = Seq(
        "flight_id","FL_DATE","CRS_DEP_TIMESTAMP","CRS_ELAPSED_TIME","CRS_ARR_TIMESTAMP",
        "ORIGIN_AIRPORT_ID","DEST_AIRPORT_ID","ARR_DELAY_NEW"
      ) ++ Seq("OP_UNIQUE_CARRIER","OP_CARRIER","OP_CARRIER_FL_NUM","OP_CARRIER_AIRLINE_ID").filter(present.contains)

      val flightsBase = flights.select(baseColsFixed.map(col): _*)

      val joinedCore =
        if (lags > 0) flightsBase.join(originAgg, "flight_id").join(destAgg, "flight_id")
        else          flightsBase.join(originAgg, "flight_id").join(destAgg, "flight_id")

      val joined = joinedCore.select(
        (baseColsFixed.filter(_ != "flight_id").map(col) :+
          col(if (lags > 0) "weather_origin" else "weather_origin_single")) :+
          col(if (lags > 0) "weather_dest"   else "weather_dest_single") : _*
      )

      // 5bis) Écriture de l'intermédiaire et coupure du DAG
      sc.setJobDescription("Write intermediate Delta (cut DAG)")
      joined.write.format("delta").mode("overwrite").save(outIntermediate)
      log.info(s"[Join] Intermédiaire (Delta) → $outIntermediate")

      // -----------------------------
      // 6) Aplatissement + agrégats (DÉSACTIVÉ)
      // -----------------------------
      /*
      sc.setJobDescription(s"Flatten + aggregates (lags=$lags)")
      val joinedMat = spark.read.format("delta").load(outIntermediate)

      val originStruct = joinedMat.schema("weather_origin").dataType.asInstanceOf[ArrayType]
        .elementType.asInstanceOf[StructType]

      val numericFields = originStruct.fields
        .filter(f => f.dataType.isInstanceOf[NumericType])
        .map(_.name)
        .filter(_ != "w_ts")

      val flatBase = joinedMat.columns.filterNot(c => c == "weather_origin" || c == "weather_dest").map(col)

      val origLagCols: Seq[Column] =
        (0 until lags).flatMap { k =>
          featureCols.map(f => col("weather_origin").getItem(k).getField(f).alias(s"orig_${f}_lag$k")) ++
            Seq(col("weather_origin").getItem(k).getField("w_ts").alias(s"orig_wts_lag$k"))
        }

      val destLagCols: Seq[Column] =
        (0 until lags).flatMap { k =>
          featureCols.map(f => col("weather_dest").getItem(k).getField(f).alias(s"dest_${f}_lag$k")) ++
            Seq(col("weather_dest").getItem(k).getField("w_ts").alias(s"dest_wts_lag$k"))
        }

      val flat = joinedMat.select((flatBase ++ origLagCols ++ destLagCols): _*)

      def aggCols(prefix: String, f: String): Seq[Column] = {
        val kCols = (0 until lags).map(k => s"${prefix}_${f}_lag$k")
        val minC = array_min(array(kCols.map(c => col(c)):_*)).alias(s"${prefix}_${f}_min")
        val maxC = array_max(array(kCols.map(c => col(c)):_*)).alias(s"${prefix}_${f}_max")
        val sumExpr = kCols.mkString(" + ")
        val avgC = expr(s"( $sumExpr ) / $lags").alias(s"${prefix}_${f}_avg")
        val deltaC = (col(s"${prefix}_${f}_lag0") - col(s"${prefix}_${f}_lag${lags - 1}")).alias(s"${prefix}_${f}_delta")
        Seq(minC, maxC, avgC, deltaC)
      }
      val originAggCols = numericFields.flatMap(f => aggCols("orig", f))
      val destAggCols   = numericFields.flatMap(f => aggCols("dest", f))

      val flatAgg = flat.select( (flat.columns.map(col) ++ originAggCols ++ destAggCols): _* )

      val bad = "[ ,;{}()\\n\\t=]+".r
      val cols = flatAgg.columns.map(c => bad.replaceAllIn(c, "_"))
      val sanitized = flatAgg.toDF(cols: _*)

      sanitized.write.format("delta").mode("overwrite").save(outFlat)
      log.info(s"[Join] Flat + Agg (lags=$lags) → $outFlat")
      */

      // libère la mémoire
      wSelAllPersisted.foreach(_.unpersist())
      flightsPersisted.foreach(_.unpersist())

      // retourne l'intermédiaire tel qu'écrit
      spark.read.format("delta").load(outIntermediate)

    } finally {
      val sc2 = spark.sparkContext
      sc2.setJobDescription(null)
      sc2.clearJobGroup()
    }
  }
}
