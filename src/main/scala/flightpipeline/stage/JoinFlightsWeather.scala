package flightpipeline.stage

import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{ArrayType, StructType, NumericType}
import org.slf4j.LoggerFactory

final class JoinFlightsWeather(
                                spark: SparkSession,
                                flightCleanPath: String,
                                weatherCleanPath: String,
                                outIntermediate: String,
                                outFlat: String,
                                windowHours: Int,
                                lags: Int
                              ) {

  private val log = LoggerFactory.getLogger(getClass)

  /** Sélectionne les colonnes météo :
   *  - drop toute MAP éventuelle (weather_scores)
   *  - applique une WHITELIST "riche" (alignée article)
   *  - renvoie (DF sélectionné, liste des features hors timestamp/w_ts)
   */
  private def selectWeatherColumns(weatherRaw: DataFrame): (DataFrame, Seq[String]) = {
    val base0 =
      if (weatherRaw.columns.contains("weather_scores")) weatherRaw.drop("weather_scores")
      else weatherRaw

    // Whitelist riche (ajuste si besoin)
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
    val flights = spark.read.format("delta").load(flightCleanPath)
    val weather = spark.read.format("delta").load(weatherCleanPath)

    val keyColNames: Seq[String] = flights.columns.toSeq
    val keyCols: Seq[Column]     = keyColNames.map(col)

    val flightsTS = flights.withColumn(
      "CRS_ARR_TIMESTAMP",
      (col("CRS_DEP_TIMESTAMP").cast("long") + (col("CRS_ELAPSED_TIME") * 60).cast("long")).cast("timestamp")
    )

    val (wSelAll, featureCols0) = selectWeatherColumns(weather)
    val featureCols = featureCols0.filter(_ != "w_ts") // on ne garde pas w_ts comme feature
    log.info(s"[Join] #features météo utilisées (hors w_ts): ${featureCols.size}; lags=$lags")

    // -------- ORIGINE (tsd - window .. tsd), on prend les lags plus récents d'abord
    val wOrig = wSelAll.withColumnRenamed("apt_id", "orig_apt")
      .withColumnRenamed("w_ts",  "orig_w_ts")

    val origJoin = flightsTS.join(
      wOrig,
      flightsTS("ORIGIN_AIRPORT_ID") === col("orig_apt") &&
        col("orig_w_ts") <= flightsTS("CRS_DEP_TIMESTAMP") &&
        col("orig_w_ts") >= (flightsTS("CRS_DEP_TIMESTAMP") - expr(s"INTERVAL $windowHours HOURS")),
      "left"
    )

    val wDescOrig = Window.partitionBy(keyCols: _*).orderBy(col("orig_w_ts").desc)

    val origLimited =
      if (lags > 0) {
        // Prendre les lags plus récents (borne = lags)
        origJoin.withColumn("rk", row_number().over(wDescOrig))
          .filter(col("rk") <= lags)
          .drop("rk")
      } else {
        // cas théorique lags==0 : on garde tout de même 1 obs
        origJoin.withColumn("rk", row_number().over(wDescOrig))
          .filter(col("rk") === 1).drop("rk")
      }

    val withOrigin =
      if (lags > 0) {
        origLimited.groupBy(keyCols: _*)
          .agg(
            array_sort(
              collect_list( struct( (col("orig_w_ts").as("w_ts") +: featureCols.map(col)):_* ) )
            ).as("weather_origin")
          )
      } else {
        origLimited
          .select(
            keyCols :+ struct( (col("orig_w_ts").as("w_ts") +: featureCols.map(col)):_* ).as("weather_origin_single") : _*
          )
      }

    // -------- DESTINATION (tsa - window .. tsa)
    val wDest = wSelAll.withColumnRenamed("apt_id", "dest_apt")
      .withColumnRenamed("w_ts",  "dest_w_ts")

    val destJoin = flightsTS.join(
      wDest,
      flightsTS("DEST_AIRPORT_ID") === col("dest_apt") &&
        col("dest_w_ts") <= col("CRS_ARR_TIMESTAMP") &&
        col("dest_w_ts") >= (col("CRS_ARR_TIMESTAMP") - expr(s"INTERVAL $windowHours HOURS")),
      "left"
    )

    val wDescDest = Window.partitionBy(keyCols: _*).orderBy(col("dest_w_ts").desc)
    val destLimited =
      if (lags > 0) {
        destJoin.withColumn("rk", row_number().over(wDescDest))
          .filter(col("rk") <= lags)
          .drop("rk")
      } else {
        destJoin.withColumn("rk", row_number().over(wDescDest))
          .filter(col("rk") === 1).drop("rk")
      }

    val destAgg =
      if (lags > 0) {
        destLimited.groupBy(keyCols: _*)
          .agg(
            array_sort(
              collect_list( struct( (col("dest_w_ts").as("w_ts") +: featureCols.map(col)):_* ) )
            ).as("weather_dest")
          )
      } else {
        destLimited
          .select(
            keyCols :+ struct( (col("dest_w_ts").as("w_ts") +: featureCols.map(col)):_* ).as("weather_dest_single") : _*
          )
      }

    // -------- Fusion
    val joined =
      if (lags > 0) {
        withOrigin.join(destAgg, keyColNames, "outer")
      } else {
        withOrigin.join(destAgg, keyColNames, "outer")
      }

    // Écrit l'intermédiaire (toujours)
    joined.write.format("delta").mode("overwrite").save(outIntermediate)
    log.info(s"[Join] Intermédiaire (Delta) → $outIntermediate  rows=${joined.count()}")

    // -------- Aplatissement + agrégats si lags > 0
    if (lags > 0) {
      // Types numériques disponibles dans la STRUCT météo (hors w_ts)
      val originStruct = joined.schema("weather_origin").dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val numericFields = originStruct.fields
        .filter(f => f.dataType.isInstanceOf[NumericType])
        .map(_.name)
        .filter(_ != "w_ts")
        .filter(featureCols.contains)

      // Colonnes lag (orig/dest)
      val baseCols = joined.columns.filterNot(c => c == "weather_origin" || c == "weather_dest").map(col)

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

      val flat = joined.select((baseCols ++ origLagCols ++ destLagCols): _*)

      // agrégats min/max/avg/delta pour numériques (origine & destination)
      def aggCols(prefix: String, f: String): Seq[Column] = {
        val head = s"${prefix}_${f}_lag0"
        val tail = s"${prefix}_${f}_lag${lags - 1}"
        Seq(
          array_min(array((0 until lags).map(k => col(s"${prefix}_${f}_lag$k")): _*)).alias(s"${prefix}_${f}_min"),
          array_max(array((0 until lags).map(k => col(s"${prefix}_${f}_lag$k")): _*)).alias(s"${prefix}_${f}_max"),
          ( (0 until lags).map(k => col(s"${prefix}_${f}_lag$k")).reduce(_ + _) / lit(lags) ).alias(s"${prefix}_${f}_avg"),
          (col(head) - col(tail)).alias(s"${prefix}_${f}_delta")
        )
      }

      val originAgg = numericFields.flatMap(f => aggCols("orig", f))
      val destAgg   = numericFields.flatMap(f => aggCols("dest", f))

      val flatAgg = flat.select( (flat.columns.map(col) ++ originAgg ++ destAgg): _* )

      flatAgg.write.format("delta").mode("overwrite").save(outFlat)
      log.info(s"[Join] Flat + Agg (lags=$lags) → $outFlat  rows=${flatAgg.count()}")

      // On peut détacher la météo sélectionnée si elle a été cache ailleurs
    }

    joined
  }
}
