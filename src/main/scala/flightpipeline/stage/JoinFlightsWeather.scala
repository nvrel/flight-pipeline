package flightpipeline.stage

import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.slf4j.LoggerFactory

/** Jointure temporelle Vols <-> Météo (origine & destination) :
 *  - lags == 0 : 1 observation (la plus récente) par côté -> STRUCT (pas d'ARRAY)
 *  - lags > 0  : liste triée bornée à `lags` -> ARRAY<STRUCT>
 *  - fusion via usingColumns (Seq[String])
 *  - écriture intermédiaire + aplat (si lags > 0)
 */
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

  /** Sélection des colonnes météo utilisables pour la jointure/struct :
   *  - supprime explicitement toute éventuelle MAP "weather_scores"
   *  - (option) whitelist : décommente pour réduire encore la largeur des structs
   *  - renomme: AirportId -> apt_id, timestamp -> w_ts
   */
  private def selectWeatherColumns(weatherRaw: DataFrame): (DataFrame, Seq[String]) = {
    // Sécurité : si la MAP existe encore, on la supprime.
    val base0 = if (weatherRaw.columns.contains("weather_scores")) weatherRaw.drop("weather_scores") else weatherRaw

    // --- (Option) whitelist pour réduire la mémoire :
    // Décommente et adapte si tu veux limiter fortement les features météo.
    // val whitelist = Set(
    //   "timestamp","DryBulbFarenheit","Visibility","WindSpeed","WindDirection",
    //   "Altimeter","SeaLevelPressure","HourlyPrecip",
    //   "sky_num_layers","sky_min_altitude","sky_max_altitude","sky_mean_altitude",
    //   "sky_has_CB","sky_has_TCU","sky_has_OVC","sky_has_BKN","sky_has_SCT","sky_has_FEW","sky_has_VV",
    //   "wt_score_RA","wt_score_TS","wt_score_FG","wt_score_BR","wt_score_FZ"
    // )
    // val base = base0.select((Seq("AirportId") ++ whitelist).map(col): _*)

    val base = base0 // <- version "large" (toutes les colonnes utiles)
    val cols = base.columns.filter(_ != "AirportId")

    val selected = base.select(
      col("AirportId").alias("apt_id") +:
        cols.map {
          case "timestamp" => col("timestamp").alias("w_ts")
          case other       => col(other)
        }: _*
    )
    val featureCols = selected.columns.filterNot(c => c == "apt_id" || c == "w_ts")
    (selected, featureCols)
  }

  def run(): DataFrame = {
    log.info(s"[Join] flights=$flightCleanPath ; weather=$weatherCleanPath ; hours=$windowHours ; lags=$lags")
    // LECTURES DELTA
    val flights = spark.read.format("delta").load(flightCleanPath)
    val weather = spark.read.format("delta").load(weatherCleanPath)

    // Clés de jointure/agrégat
    val keyColNames: Seq[String] = flights.columns.toSeq
    val keyCols: Seq[Column]     = keyColNames.map(col)

    // Timestamp d'arrivée planifiée (dép + durée planifiée en minutes)
    val flightsTS = flights.withColumn(
      "CRS_ARR_TIMESTAMP",
      (col("CRS_DEP_TIMESTAMP").cast("long") + (col("CRS_ELAPSED_TIME") * 60).cast("long")).cast("timestamp")
    )

    val (wSel, featureCols) = selectWeatherColumns(weather)
    log.info(s"[Join] #features météo utilisées: ${featureCols.length}")

    /** Helper : renvoie un DataFrame "originAgg" OU "destAgg"
     *  - si lags == 0 : STRUCT unique (colonne weather_*_single)
     *  - si lags > 0  : ARRAY<STRUCT> borné à lags (colonne weather_*)
     */
    def buildSide(
                   sideName: String,                 // "origin" | "dest"
                   joinAptCol: Column,               // ORIGIN_AIRPORT_ID ou DEST_AIRPORT_ID
                   refTsCol: Column,                 // CRS_DEP_TIMESTAMP ou CRS_ARR_TIMESTAMP
                   wTsColName: String,               // "orig_w_ts" | "dest_w_ts"
                   aptColName: String                // "orig_apt"   | "dest_apt"
                 ): DataFrame = {
      val wSide = wSel
        .withColumnRenamed("apt_id", aptColName)
        .withColumnRenamed("w_ts",  wTsColName)

      val joined = flightsTS.join(
        wSide,
        joinAptCol === col(aptColName) &&
          col(wTsColName) >= (refTsCol - expr(s"INTERVAL $windowHours HOURS")) &&
          col(wTsColName) <= refTsCol,
        "left"
      )

      val wDesc   = Window.partitionBy(keyCols: _*).orderBy(col(wTsColName).desc)
      val lagName = if (sideName == "origin") "weather_origin" else "weather_dest"

      if (lags == 0) {
        // --- 1 seule observation : la plus récente (STRUCT, pas d'ARRAY)
        joined
          .withColumn("rn", row_number().over(wDesc))
          .filter(col("rn") === 1)
          .drop("rn")
          .select(
            keyCols :+
              struct(col(wTsColName).as("w_ts") +: featureCols.map(col): _*).alias(s"${lagName}_single")
              : _*)
      } else {
        // --- Liste triée bornée : on limite à `lags` d'abord, puis on agrège
        val limited = joined
          .withColumn("rk", row_number().over(wDesc))
          .filter(col("rk") <= lags)
          .drop("rk")

        limited
          .groupBy(keyCols: _*)
          .agg(
            array_sort(
              collect_list(
                struct(col(wTsColName).as("w_ts") +: featureCols.map(col): _*)
              )
            ).as(lagName)
          )
      }
    }

    // Origine : fenêtre autour du départ
    val originAgg = buildSide(
      sideName   = "origin",
      joinAptCol = flightsTS("ORIGIN_AIRPORT_ID"),
      refTsCol   = flightsTS("CRS_DEP_TIMESTAMP"),
      wTsColName = "orig_w_ts",
      aptColName = "orig_apt"
    )

    // Destination : fenêtre autour de l'arrivée planifiée
    val destAgg = buildSide(
      sideName   = "dest",
      joinAptCol = flightsTS("DEST_AIRPORT_ID"),
      refTsCol   = flightsTS("CRS_ARR_TIMESTAMP"),
      wTsColName = "dest_w_ts",
      aptColName = "dest_apt"
    )

    // Fusion via usingColumns (Seq[String]) -- c'est la bonne surcharge
    val joined = originAgg.join(destAgg, keyColNames, "outer")

    // Écriture intermédiaire :
    if (lags == 0) {
      joined.write.format("delta").mode("overwrite").save(outIntermediate)
      log.info(s"[Join] Intermédiaire (single structs) [DELTA] → $outIntermediate (rows=${joined.count()})")
      joined
    } else {
      joined.write.format("delta").mode("overwrite").save(outIntermediate)
      log.info(s"[Join] Intermédiaire (arrays ≤ $lags) [DELTA] → $outIntermediate (rows=${joined.count()})")

      val originCols: Seq[Column] =
        (0 until lags).flatMap { k =>
          featureCols.map { f =>
            when(size(col("weather_origin")) > k, col("weather_origin").getItem(k).getField(f))
              .otherwise(lit(null)).alias(s"orig_${f}_lag$k")
          }
        }

      val destCols: Seq[Column] =
        (0 until lags).flatMap { k =>
          featureCols.map { f =>
            when(size(col("weather_dest")) > k, col("weather_dest").getItem(k).getField(f))
              .otherwise(lit(null)).alias(s"dest_${f}_lag$k")
          }
        }

      val flat = joined.select(
        joined.columns.filterNot(c => c == "weather_origin" || c == "weather_dest").map(col) ++
          originCols ++ destCols: _*
      )
      flat.write.format("delta").mode("overwrite").save(outFlat)
      log.info(s"[Join] Aplat lags=$lags [DELTA] → $outFlat (rows=${flat.count()})")
      flat
    }
  }
}
