package flightpipeline.stage

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/** Contrôles qualité simples sur les datasets "clean".
 * Produit :
 *  - out/quality/quality_summary.json  (une ligne par dataset)
 *  - out/quality/quality_summary.csv   (idem CSV)
 */
final class QualityCheck(
                          spark: SparkSession,
                          flightCleanPath: String,
                          weatherCleanPath: String,
                          airportTzPath: String,
                          outDir: String
                        ) {
  private val log = LoggerFactory.getLogger(getClass)

  // Schéma cible standardisé (toutes les colonnes possibles)
  private val targetCols: Seq[(String, DataType)] = Seq(
    "dataset"                         -> StringType,
    "rows"                            -> LongType,
    // flights
    "non_null_CRS_DEP_TIMESTAMP"      -> LongType,
    "non_null_ORIGIN_AIRPORT_ID"      -> LongType,
    "non_null_DEST_AIRPORT_ID"        -> LongType,
    "distinct_FL_DATE"                -> LongType,
    "distinct_ORIGIN_AIRPORT_ID"      -> LongType,
    "distinct_DEST_AIRPORT_ID"        -> LongType,
    // weather
    "non_null_timestamp"              -> LongType,
    "non_null_AirportId"              -> LongType,
    "distinct_AirportId"              -> LongType,
    "distinct_date"                   -> LongType,
    // airport tz
    "distinct_WBAN"                   -> LongType
  )

  private def normalize(df: DataFrame): DataFrame = {
    // ajoute les colonnes manquantes et cast aux bons types
    val withAll = targetCols.foldLeft(df) {
      case (acc, (name, dt)) =>
        if (acc.columns.contains(name)) acc.withColumn(name, col(name).cast(dt))
        else acc.withColumn(name, lit(null).cast(dt))
    }
    // réordonne les colonnes
    withAll.select(targetCols.map{ case (n, _) => col(n) }: _*)
  }

  private def summarizeFlights(df: DataFrame): DataFrame = {
    val metrics = Seq(
      count(lit(1)).alias("rows"),
      count(col("CRS_DEP_TIMESTAMP")).alias("non_null_CRS_DEP_TIMESTAMP"),
      count(col("ORIGIN_AIRPORT_ID")).alias("non_null_ORIGIN_AIRPORT_ID"),
      count(col("DEST_AIRPORT_ID")).alias("non_null_DEST_AIRPORT_ID"),
      approx_count_distinct(col("FL_DATE")).alias("distinct_FL_DATE"),
      approx_count_distinct(col("ORIGIN_AIRPORT_ID")).alias("distinct_ORIGIN_AIRPORT_ID"),
      approx_count_distinct(col("DEST_AIRPORT_ID")).alias("distinct_DEST_AIRPORT_ID")
    )
    val base = df.agg(metrics.head, metrics.tail: _*).withColumn("dataset", lit("flight_clean"))
    normalize(base)
  }

  private def summarizeWeather(df: DataFrame): DataFrame = {
    val metrics = Seq(
      count(lit(1)).alias("rows"),
      count(col("timestamp")).alias("non_null_timestamp"),
      count(col("AirportId")).alias("non_null_AirportId"),
      approx_count_distinct(col("AirportId")).alias("distinct_AirportId"),
      approx_count_distinct(to_date(col("timestamp"))).alias("distinct_date")
    )
    val base = df.agg(metrics.head, metrics.tail: _*).withColumn("dataset", lit("weather_clean"))
    normalize(base)
  }

  private def summarizeAirportTz(df: DataFrame): DataFrame = {
    val metrics = Seq(
      count(lit(1)).alias("rows"),
      approx_count_distinct(col("AirportId")).alias("distinct_AirportId"),
      approx_count_distinct(col("WBAN")).alias("distinct_WBAN")
    )
    val base = df.agg(metrics.head, metrics.tail: _*).withColumn("dataset", lit("airport_timezone_clean"))
    normalize(base)
  }

  def run(): Unit = {
    log.info(s"[Quality] Lecture (DELTA) : flights=$flightCleanPath ; weather=$weatherCleanPath ; airportTZ=$airportTzPath")

    val flights = spark.read.format("delta").load(flightCleanPath)
    val weather = spark.read.format("delta").load(weatherCleanPath)
    val aptTz   = spark.read.format("delta").load(airportTzPath)

    val sumFlights = summarizeFlights(flights)
    val sumWeather = summarizeWeather(weather)
    val sumAptTz   = summarizeAirportTz(aptTz)

    // Union tolérante aux colonnes manquantes (on a déjà normalisé mais on garde true par sécurité)
    val summary = sumFlights.unionByName(sumWeather, allowMissingColumns = true)
      .unionByName(sumAptTz,   allowMissingColumns = true)

    summary.coalesce(1).write.mode("overwrite").json(s"$outDir/quality_summary.json")
    summary.coalesce(1).write.mode("overwrite").option("header","true").csv(s"$outDir/quality_summary.csv")

    log.info(s"[Quality] Rapports écrits → $outDir (JSON + CSV)")
  }
}
