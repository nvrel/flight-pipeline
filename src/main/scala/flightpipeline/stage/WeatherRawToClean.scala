package flightpipeline.stage

import flightpipeline.util.ParsingUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/** Préparation des données météo (QCLCD) avec restriction stricte :
 *  - WBAN conservés = WBAN rattachés aux AirportId réellement présents dans flight_clean
 *  - nettoyage / typage, features SkyCondition, WeatherType -> wt_score_*, DROP de la MAP
 *  - rattachement WBAN -> AirportId
 *  - sortie Delta "weather_clean"
 */
final class WeatherRawToClean(
                               spark: SparkSession,
                               weatherDir: String,
                               airportCsv: String,
                               flightCleanPath: String,    // <-- NOUVEAU : chemin Delta du flight_clean
                               weatherOut: String,
                               airportOut: String
                             ) {

  private val log = LoggerFactory.getLogger(getClass)

  /** Construit le référentiel AirportId <-> WBAN <-> TimeZone à partir du CSV (Delta). */
  private def buildAirportTimezone(): DataFrame = {
    log.info(s"[AirportTZ] Lecture CSV mapping: $airportCsv")
    val df = spark.read
      .option("header","true")
      .option("inferSchema","false")
      .csv(airportCsv)

    val cleaned = df
      .select(
        col("AirportId").cast(ShortType).as("AirportId"),
        col("WBAN").cast(IntegerType).as("WBAN"),
        col("TimeZone").cast(ByteType).as("TimeZone")
      )
      .na.drop(Seq("AirportId","WBAN"))
      .dropDuplicates("AirportId","WBAN")

    cleaned.write.format("delta").mode("overwrite").save(airportOut)
    log.info(s"[AirportTZ] Écrit (DELTA) → $airportOut (rows=${cleaned.count()})")
    cleaned
  }

  def run(): DataFrame = {
    // 1) Référentiel AirportId<->WBAN
    val airportTZ = buildAirportTimezone().cache()

    // 2) Récupérer la liste des AirportId réellement utilisés dans flight_clean (Delta)
    log.info(s"[Weather] Lecture flight_clean (Delta) pour restreindre les WBAN) : $flightCleanPath")
    val flightClean = spark.read.format("delta").load(flightCleanPath)
    val airportIdsUsed = flightClean
      .select(col("ORIGIN_AIRPORT_ID").as("AirportId"))
      .union(flightClean.select(col("DEST_AIRPORT_ID").as("AirportId")))
      .distinct()
      .cache()
    log.info(s"[Weather] AirportId distincts utilisés dans flights: ${airportIdsUsed.count()}")

    // 3) Restreindre le mapping aux seuls AirportId utilisés → WBAN autorisés
    val wbanAllowed = airportTZ
      .join(airportIdsUsed, Seq("AirportId"), "inner")
      .select(col("WBAN").cast(IntegerType).as("WBAN"), col("AirportId"))
      .distinct()
      .cache()
    log.info(s"[Weather] WBAN autorisés (rattachés à des AirportId présents): ${wbanAllowed.count()}")

    // 4) Lecture Météo brute (QCLCD) + exclusion des colonnes Celsius
    log.info(s"[Weather] Lecture TXT (QCLCD) depuis $weatherDir")
    val raw = spark.read
      .option("header","true")
      .option("inferSchema","false")
      .csv(weatherDir)   // <-- on lit exactement ce qu'on reçoit (chemin ou glob)
    val keptCols = raw.columns.filterNot(_.contains("Celsius"))
    val df0 = raw.select(keptCols.map(col): _*)

    // 5) Parsing date+time -> timestamp + normalisation des numériques ("M" / "" -> null)
    val df1 = df0
      .withColumn("WBAN", nullIfMissing(col("WBAN")).cast(IntegerType))
      .withColumn("time", lpad(col("time"), 4, "0"))
      .withColumn("timestamp", toTimestampFromDateTime(col("date"), col("time")))
      .withColumn("DryBulbFarenheit", nullIfMissing(col("DryBulbFarenheit")).cast(FloatType))
      .withColumn("WetBulbFarenheit", nullIfMissing(col("WetBulbFarenheit")).cast(FloatType))
      .withColumn("DewPointFarenheit", nullIfMissing(col("DewPointFarenheit")).cast(FloatType))
      .withColumn("RelativeHumidity", nullIfMissing(col("RelativeHumidity")).cast(ShortType))
      .withColumn("WindSpeed",        nullIfMissing(col("WindSpeed")).cast(ShortType))
      .withColumn("WindDirection",    nullIfMissing(col("WindDirection")).cast(ShortType))
      .withColumn("ValueForWindCharacter", nullIfMissing(col("ValueForWindCharacter")).cast(ByteType))
      .withColumn("PressureTendency", nullIfMissing(col("PressureTendency")).cast(ByteType))
      .withColumn("PressureChange",   nullIfMissing(col("PressureChange")).cast(ShortType))
      .withColumn("Altimeter",        nullIfMissing(col("Altimeter")).cast(FloatType))
      .withColumn("SeaLevelPressure", nullIfMissing(col("SeaLevelPressure")).cast(FloatType))
      .withColumn("StationPressure",  nullIfMissing(col("StationPressure")).cast(FloatType))
      .withColumn("HourlyPrecip",
        when(trim(col("HourlyPrecip")) === "T", lit(null).cast(FloatType))
          .otherwise(nullIfMissing(col("HourlyPrecip")).cast(FloatType))
      )
      .withColumn("SkyCondition", col("SkyCondition"))
      .withColumn("WeatherType",  col("WeatherType"))
      .drop("date","time")

    // 6) RESTREINDRE ICI la météo aux WBAN autorisés
    val dfRestricted = df1.join(wbanAllowed, Seq("WBAN"), "inner")

    // 7) Features SkyCondition (couches, altitudes min/max/moy, CB/TCU + indicateurs nuageux)
    val skyPrefixes = Seq("OVC","BKN","SCT","FEW","VV")
    val withSky = dfRestricted
      .withColumn("sky_num_layers", countSeq(col("SkyCondition")).cast(ByteType))
      .withColumn("sky_altitudes",  extractAltitudes(col("SkyCondition")))
      .withColumn("sky_min_altitude", expr("aggregate(sky_altitudes, 1000000, (acc, x) -> IF(x < acc, x, acc))").cast(ShortType))
      .withColumn("sky_max_altitude", expr("aggregate(sky_altitudes, 0, (acc, x) -> IF(x > acc, x, acc))").cast(ShortType))
      .withColumn("sky_mean_altitude",
        when(size(col("sky_altitudes")) > 0,
          expr("aggregate(sky_altitudes, 0, (acc, x) -> acc + x) / size(sky_altitudes)")
        ).cast(FloatType)
      )
      .withColumn("sky_has_CB",  hasCB(col("SkyCondition")).cast(BooleanType))
      .withColumn("sky_has_TCU", hasTCU(col("SkyCondition")).cast(BooleanType))
      .drop("sky_altitudes")

    val withSkyFlags = skyPrefixes.foldLeft(withSky) { (acc, p) =>
      acc.withColumn(s"sky_has_$p", col("SkyCondition").contains(p).cast(BooleanType))
    }

    // 8) WeatherType -> scores pondérés + drop de la MAP
    val withWt = withSkyFlags.withColumn("weather_scores", extractWeatherScores(col("WeatherType")))
    val withScoresOnly = flightpipeline.util.ParsingUtils.weatherPairs.foldLeft(withWt) { (df, p) =>
      df.withColumn(s"wt_score_$p", coalesce(col("weather_scores")(p), lit(0)))
    }.drop("weather_scores") // suppression définitive de la MAP

    // 9) Pas de 2e join avec wbanAllowed : AirportId est déjà présent depuis dfRestricted
    //    On se contente de typer et de filtrer proprement.
    val joined =
      withScoresOnly
        .withColumn("AirportId", col("AirportId").cast(ShortType))
        .filter(col("AirportId").isNotNull)

    joined.write.format("delta").mode("overwrite").save(weatherOut)
    log.info(s"[Weather] Écrit (DELTA, restreint) → $weatherOut (rows=${joined.count()})")

    airportIdsUsed.unpersist(false)
    wbanAllowed.unpersist(false)
    airportTZ.unpersist(false)

    joined }
}
