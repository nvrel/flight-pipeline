package flightpipeline.stage

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

final class FlightsRawToClean(spark: SparkSession, flightsGlob: String, out: String) {

  private val log = LoggerFactory.getLogger(getClass)

  def run(): DataFrame = {
    log.info(s"[Flights] Lecture CSV depuis $flightsGlob")

    // IMPORTANT : on lit le chemin/glob tel quel (pas d’ajout de /*.csv ici)
    val dfRaw = spark.read
      .option("header","true")
      .option("inferSchema","false")
      .csv(flightsGlob)

    val keep = Seq(
      "FL_DATE","OP_CARRIER","OP_CARRIER_FL_NUM",
      "ORIGIN_AIRPORT_ID","DEST_AIRPORT_ID",
      "CRS_DEP_TIME","CRS_ELAPSED_TIME",
      "ARR_DELAY_NEW","WEATHER_DELAY","NAS_DELAY","CANCELLED","DIVERTED"
    ).filter(dfRaw.columns.contains)

    val base = dfRaw.select(keep.map(col): _*)

    val filtered = base
      .filter(!col("CANCELLED").equalTo(1.0) || !base.columns.contains("CANCELLED"))
      .filter(!col("DIVERTED").equalTo(1.0)  || !base.columns.contains("DIVERTED"))

    val cleaned = filtered
      .withColumn("FL_DATE", to_date(col("FL_DATE"), "yyyy-MM-dd"))
      .withColumn("OP_CARRIER_FL_NUM", col("OP_CARRIER_FL_NUM").cast(ShortType))
      .withColumn("ORIGIN_AIRPORT_ID", col("ORIGIN_AIRPORT_ID").cast(ShortType))
      .withColumn("DEST_AIRPORT_ID",   col("DEST_AIRPORT_ID").cast(ShortType))
      .withColumn("ARR_DELAY_NEW",     col("ARR_DELAY_NEW").cast(FloatType))
      .withColumn("CRS_ELAPSED_TIME",  col("CRS_ELAPSED_TIME").cast(FloatType))
      .withColumn("HAS_WEATHER_DELAY", col("WEATHER_DELAY").isNotNull)
      .withColumn("WEATHER_DELAY",     coalesce(col("WEATHER_DELAY").cast(FloatType), lit(0.0f)))
      .withColumn("HAS_NAS_DELAY",     col("NAS_DELAY").isNotNull)
      .withColumn("NAS_DELAY",         coalesce(col("NAS_DELAY").cast(FloatType), lit(0.0f)))
      .withColumn("CRS_DEP_TIME_STR",  lpad(col("CRS_DEP_TIME").cast(StringType), 4, "0"))
      .withColumn("CRS_DEP_TIMESTAMP", to_timestamp(concat(col("FL_DATE"), col("CRS_DEP_TIME_STR")), "yyyy-MM-ddHHmm"))
      .drop("CRS_DEP_TIME_STR","CANCELLED","DIVERTED")

    cleaned.write.format("delta").mode("overwrite").save(out)
    val nOut = cleaned.count()
    log.info(s"[Flights] Écrit $nOut lignes (DELTA) → $out")
    cleaned
  }
}
