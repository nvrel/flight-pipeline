package flightpipeline

import com.typesafe.config.ConfigFactory
import flightpipeline.config.Args
import flightpipeline.io.DataPaths
import flightpipeline.stage._
import flightpipeline.report.ShowTrainingRuns
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import flightpipeline.eval.DelayDataset


object Main {
  private val log = LoggerFactory.getLogger(getClass)

  def main(rawArgs: Array[String]): Unit = {
    // --------------------------------------------------------------------
    // 1) Chargement de la configuration par défaut (application.conf)
    // --------------------------------------------------------------------
    val conf = ConfigFactory.load().getConfig("app")

    // Valeurs par défaut : peuvent être surchargées par la ligne de commande
    // via Args.parse (ex. --mode join --lags 7 --delay-threshold 60)
    val defaults = Args(
      flightsDir            = conf.getString("flights-dir"),
      weatherDir            = conf.getString("weather-dir"),
      airportCsv            = conf.getString("airport-csv"),
      outRoot               = conf.getString("out-root"),
      windowHours           = conf.getInt("window-hours"),
      lags                  = conf.getInt("lags"),
      delayThresholdMinutes = conf.getInt("delay-threshold-minutes"),
      mode                  = "all",
      sampleMonth           = None,
      // Dataset de retards ciblé pour l’entraînement.
      // Par exemple "D3" comme dans la plupart des graphiques de l’article (section 4.2).
      delayDataset          = conf.getString("delay-dataset")
    )
    // Parsing des arguments CLI : écrase les valeurs de defaults si fourni
    val args  = Args.parse(defaults, rawArgs)
    val paths = DataPaths(args.flightsDir, args.weatherDir, args.airportCsv, args.outRoot)

    // --------------------------------------------------------------------
    // 2) Gestion du mode échantillon : restriction des glob d'entrée
    // --------------------------------------------------------------------
    val (effFlights, effWeather) = args.sampleMonth match {
      case Some(yyyymm) =>
        // Exemple : 200907 → flights/200907.csv, weather/200907hourly.txt
        val flightsGlob = s"${args.flightsDir}/$yyyymm.csv"
        val weatherGlob = s"${args.weatherDir}/${yyyymm}hourly.txt"

        log.info(s"[sample] Mode échantillon activé pour $yyyymm")
        log.info(s"[sample] Flights glob   = $flightsGlob")
        log.info(s"[sample] Weather glob   = $weatherGlob")

        (flightsGlob, weatherGlob)

      case None =>
        // Lecture complète de tous les fichiers disponibles
        (s"${args.flightsDir}/*.csv", s"${args.weatherDir}/*hourly.txt")
    }

    // --------------------------------------------------------------------
    // 3) Création de la SparkSession
    // --------------------------------------------------------------------
    val spark = SparkSession
      .builder()
      .appName("flight-pipeline")
      .getOrCreate()

    val sampleLabel = args.sampleMonth.getOrElse("ALL")

    log.info(
      s"=== flight-pipeline " +
        s"(mode=${args.mode}, hours=${args.windowHours}, lags=${args.lags}, " +
        s"delay-threshold-min=${args.delayThresholdMinutes}, " +
        s"sample=$sampleLabel) ==="
    )

    // --------------------------------------------------------------------
    // 3bis) Mode "report" : lecture des runs logués et export CSV
    //
    // Ce mode ne relance aucun traitement de préparation / jointure /
    // entraînement. Il se contente de relire out/metrics/train_runs
    // (produit par TrainRunLogger) et de générer :
    //   - un résumé dans les logs,
    //   - un export CSV détaillé dans out/metrics/train_runs_export.
    // --------------------------------------------------------------------
    if (args.mode == "report") {
      ShowTrainingRuns.run(spark, args.outRoot)

      log.info("=== Fin flight-pipeline (mode=report) ===")
      spark.stop()
      return
    }

    // --------------------------------------------------------------------
    // 4) Préparation des données brutes (flights + weather) → tables Delta "clean"
    // --------------------------------------------------------------------
    if (args.mode == "prepare" || args.mode == "all") {
      val t0 = System.currentTimeMillis()

      // FlightsRawToClean accepte un dossier glob — effFlights
      val flights = new FlightsRawToClean(spark, effFlights, paths.flightCleanOut).run()

      // WeatherRawToClean prend le dossier météo (glob), le CSV mapping, et le chemin flight_clean (Delta)
      val weather = new WeatherRawToClean(
        spark,
        effWeather,                  // glob météo restreint si sample
        args.airportCsv,
        paths.flightCleanOut,        // pour restreindre WBAN aux aéroports réellement utilisés
        paths.weatherCleanOut,
        paths.airportTimezoneCleanOut
      ).run()

      // Création des tables Delta externes dans le catalogue Spark
      spark.sql("CREATE DATABASE IF NOT EXISTS flight_project")

      spark.sql(
        s"""
           |CREATE TABLE IF NOT EXISTS flight_project.flight_clean
           |USING DELTA
           |LOCATION '${paths.flightCleanOut}'
           |""".stripMargin
      )

      spark.sql(
        s"""
           |CREATE TABLE IF NOT EXISTS flight_project.weather_clean
           |USING DELTA
           |LOCATION '${paths.weatherCleanOut}'
           |""".stripMargin
      )

      spark.sql(
        s"""
           |CREATE TABLE IF NOT EXISTS flight_project.airport_timezone_clean
           |USING DELTA
           |LOCATION '${paths.airportTimezoneCleanOut}'
           |""".stripMargin
      )

      spark.sql("SET spark.sql.cbo.enabled=true")
      // ANALYZE TABLE désactivé pour éviter les soucis avec Delta 3.2 / Spark 3.5

      // Contrôle qualité
      new QualityCheck(
        spark,
        paths.flightCleanOut,
        paths.weatherCleanOut,
        paths.airportTimezoneCleanOut,
        s"${paths.outRoot}/quality"
      ).run()

      log.info(
        s"[prepare] Terminé en ${(System.currentTimeMillis() - t0) / 1000.0}s " +
          s"(vols=${flights.count()} | meteo=${weather.count()})"
      )
    }

    // --------------------------------------------------------------------
    // 5) Mode "quality" isolé : relit les tables clean déjà présentes
    //    sans relancer toute la préparation.
    // --------------------------------------------------------------------
    if (args.mode == "quality") {
      new QualityCheck(
        spark,
        paths.flightCleanOut,
        paths.weatherCleanOut,
        paths.airportTimezoneCleanOut,
        s"${paths.outRoot}/quality"
      ).run()

      log.info("=== Fin flight-pipeline (mode=quality) ===")
      spark.stop()
      return
    }

    // --------------------------------------------------------------------
    // 6) Jointure flights + weather → table Delta join_intermediate
    // --------------------------------------------------------------------
    if (args.mode == "join" || args.mode == "all") {
      val t1 = System.currentTimeMillis()

      val joiner = new JoinFlightsWeather(
        spark                = spark,
        flightCleanPath      = paths.flightCleanOut,
        weatherCleanPath     = paths.weatherCleanOut,
        outIntermediate      = paths.joinIntermediateOut,
        outFlat              = paths.joinFlatOut(args.lags),
        windowHours          = args.windowHours,
        lags                 = args.lags
      )

      // Le résultat est écrit en Delta, pas besoin de conserver la DataFrame ici
      joiner.run()

      log.info(s"[join] Terminé (lags=${args.lags})")
      log.info(s"[join] Durée: ${(System.currentTimeMillis() - t1) / 1000.0}s")
    }

    // --------------------------------------------------------------------
    // 7) Entraînement Random Forest sur join_intermediate
    //    (les métriques détaillées sont journalisées par TrainRunLogger)
    // --------------------------------------------------------------------
    if (args.mode == "training" || args.mode == "train" || args.mode == "all") {
      val t2 = System.currentTimeMillis()

      val trainer = new TrainRandomForest(
        spark                 = spark,
        joinIntermediatePath  = paths.joinIntermediateOut,
        outRoot               = args.outRoot,
        lags                  = args.lags,
        delayThresholdMinutes = args.delayThresholdMinutes,
        delayDatasetId        = args.delayDataset
      )

      // run() entraîne le modèle, écrit les métriques détaillées
      // et journalise le run dans out/metrics/train_runs via TrainRunLogger.
      val metricsDF  = trainer.run()
      val metricsRow = metricsDF.first()

      // Correspondance avec les noms de colonnes produits par saveTestMetrics
      val accuracy      = metricsRow.getAs[Double]("accuracy")
      val recallDelayed = metricsRow.getAs[Double]("recall_pos")   // rappel classe 1 (retards)
      val recallOnTime  = metricsRow.getAs[Double]("specificity")  // rappel classe 0 (à l'heure)

      log.info(
        f"[training] Terminé : accuracy=$accuracy%.4f, " +
          f"recallOnTime=$recallOnTime%.4f, " +
          f"recallDelayed=$recallDelayed%.4f"
      )
      log.info(s"[training] Durée: ${(System.currentTimeMillis() - t2) / 1000.0}s")

      // Après chaque expérience d'entraînement, on rafraîchit la vue globale
      // des runs : lecture de out/metrics/train_runs (Delta) et écriture
      // d'un CSV consolidé dans out/metrics/train_runs_export.
      //
      // Ce bloc correspond à la logique de synthèse des résultats décrite
      // dans l’article (sections 4.3–4.4) lorsque les auteurs comparent
      // systématiquement les configurations (Acc / Reco / Recd).
      ShowTrainingRuns.run(spark, args.outRoot)
    }

    // --------------------------------------------------------------------
    // 8) Arrêt propre de l'application
    // --------------------------------------------------------------------
    log.info("=== Fin flight-pipeline ===")
    spark.stop()
  }
}
