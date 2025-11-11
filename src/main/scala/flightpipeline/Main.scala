package flightpipeline

import com.typesafe.config.ConfigFactory
import flightpipeline.config.Args
import flightpipeline.io.DataPaths
import flightpipeline.stage._
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Main {
  private val log = LoggerFactory.getLogger(getClass)

  def main(rawArgs: Array[String]): Unit = {
    val conf = ConfigFactory.load().getConfig("app")
    val defaults = Args(
      flightsDir  = conf.getString("flights-dir"),
      weatherDir  = conf.getString("weather-dir"),
      airportCsv  = conf.getString("airport-csv"),
      outRoot     = conf.getString("out-root"),
      windowHours = conf.getInt("window-hours"),
      lags        = conf.getInt("lags"),
      mode        = "all",
      sampleMonth = None
    )
    val args  = Args.parse(defaults, rawArgs)
    val paths = DataPaths(args.flightsDir, args.weatherDir, args.airportCsv, args.outRoot)

    // Si un mois échantillon est demandé, on restreint les globs d’entrée
    val (effFlights, effWeather) = args.sampleMonth match {
      case Some(yyyymm) =>
        val flightsGlob = s"${args.flightsDir}/${yyyymm}.csv"
        val weatherGlob = s"${args.weatherDir}/${yyyymm}hourly.txt"
        log.info(s"[sample] Mode échantillon activé pour $yyyymm")
        log.info(s"[sample] Flights glob   = $flightsGlob")
        log.info(s"[sample] Weather glob   = $weatherGlob")
        (flightsGlob, weatherGlob)
      case None =>
        // lecture complète (tous les fichiers) — comme avant
        (s"${args.flightsDir}/*.csv", s"${args.weatherDir}/*hourly.txt")
    }

    val spark = SparkSession.builder().appName("flight-pipeline").getOrCreate()
    log.info(s"=== flight-pipeline (mode=${args.mode}, hours=${args.windowHours}, lags=${args.lags}, sample=${args.sampleMonth.getOrElse("ALL")}) ===")

    if (args.mode == "prepare" || args.mode == "all") {
      val t0 = System.currentTimeMillis()

      // FlightsRawToClean accepte un dossier glob — on lui passe effFlights
      val flights = new FlightsRawToClean(spark, effFlights, paths.flightCleanOut).run()

      // WeatherRawToClean prend le dossier météo (glob), le CSV mapping, et le chemin flight_clean (Delta)
      val weather = new WeatherRawToClean(
        spark,
        effWeather,                  // <-- glob météo restreint si sample
        args.airportCsv,
        paths.flightCleanOut,        // pour restreindre WBAN aux aéroports réellement utilisés
        paths.weatherCleanOut,
        paths.airportTimezoneCleanOut
      ).run()

      // Créer les tables Delta externes (catalogue)
      spark.sql("CREATE DATABASE IF NOT EXISTS flight_project")

      spark.sql(s"""
        CREATE TABLE IF NOT EXISTS flight_project.flight_clean
        USING DELTA
        LOCATION '${paths.flightCleanOut}'
      """)
      spark.sql(s"""
        CREATE TABLE IF NOT EXISTS flight_project.weather_clean
        USING DELTA
        LOCATION '${paths.weatherCleanOut}'
      """)
      spark.sql(s"""
        CREATE TABLE IF NOT EXISTS flight_project.airport_timezone_clean
        USING DELTA
        LOCATION '${paths.airportTimezoneCleanOut}'
      """)

      spark.sql("SET spark.sql.cbo.enabled=true")
      //Ne pas faire d'ANALYZE TABLE sur des tables V2 (Delta 3.2 + Spark 3.5)
      //spark.sql("ANALYZE TABLE flight_project.flight_clean COMPUTE STATISTICS")
      //spark.sql("ANALYZE TABLE flight_project.weather_clean COMPUTE STATISTICS")
      //spark.sql("ANALYZE TABLE flight_project.airport_timezone_clean COMPUTE STATISTICS")

      // Contrôle qualité
      new flightpipeline.stage.QualityCheck(
        spark,
        paths.flightCleanOut,
        paths.weatherCleanOut,
        paths.airportTimezoneCleanOut,
        s"${paths.outRoot}/quality"
      ).run()

      log.info(s"[prepare] OK en ${(System.currentTimeMillis()-t0)/1000.0}s (vols=${flights.count()} | meteo=${weather.count()})")
    }

    if (args.mode == "quality") {
      new flightpipeline.stage.QualityCheck(
        spark,
        paths.flightCleanOut,
        paths.weatherCleanOut,
        paths.airportTimezoneCleanOut,
        s"${paths.outRoot}/quality"
      ).run()
      spark.stop(); return
    }

    if (args.mode == "join" || args.mode == "all") {
      val t1 = System.currentTimeMillis()
      val joiner = new flightpipeline.stage.JoinFlightsWeather(
        spark,
        paths.flightCleanOut,
        paths.weatherCleanOut,
        paths.joinIntermediateOut,
        paths.joinFlatOut(args.lags),
        args.windowHours,
        args.lags
      )
      val out = joiner.run()
      //val n = out.count()
      log.info(s"[join] Terminé (lags=${args.lags})")
      log.info(s"[join] Durée: ${(System.currentTimeMillis()-t1)/1000.0}s")
    }


    log.info("=== Fin flight-pipeline ===")
    spark.stop()
  }
}
