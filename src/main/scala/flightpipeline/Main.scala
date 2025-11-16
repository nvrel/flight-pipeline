package flightpipeline

import com.typesafe.config.ConfigFactory
import flightpipeline.config.Args
import flightpipeline.io.DataPaths
import flightpipeline.stage._
import flightpipeline.report.ShowTrainingRuns
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Main {
  private val log = LoggerFactory.getLogger(getClass)

  def main(rawArgs: Array[String]): Unit = {
    // --------------------------------------------------------------------
    // 1) Chargement de la configuration par défaut (application.conf)
    // --------------------------------------------------------------------
    val conf = ConfigFactory.load().getConfig("app")

    // Valeurs par défaut : peuvent être surchargées par la ligne de commande
    // via Args.parse (ex. --mode join --lags 7 --delay-threshold 60).
    //
    // Pour delay-dataset et feature-set :
    //   - delay-dataset : D1, D2, D3, D4 ou ALL (section 4.2 de l’article),
    //   - feature-set   : with-weather / no-weather (modèle avec ou sans météo).
    val defaultDelayDataset =
      if (conf.hasPath("delay-dataset")) conf.getString("delay-dataset") else "D2"

    val defaultFeatureSet =
      if (conf.hasPath("feature-set")) conf.getString("feature-set") else "with-weather"

    val defaults = Args(
      flightsDir            = conf.getString("flights-dir"),
      weatherDir            = conf.getString("weather-dir"),
      airportCsv            = conf.getString("airport-csv"),
      outRoot               = conf.getString("out-root"),
      windowHours           = conf.getInt("window-hours"),
      lags                  = conf.getInt("lags"),
      delayThresholdMinutes = conf.getInt("delay-threshold-minutes"),
      mode                  = "all",          // pipeline complet par défaut
      sampleMonth           = None,           // pas de restriction temporelle par défaut
      delayDataset          = defaultDelayDataset,
      featureSet            = defaultFeatureSet
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

    // Libellé lisible de l’étendue temporelle utilisée (ALL ou YYYYMM)
    val sampleLabel = args.sampleMonth.getOrElse("ALL")

    log.info(
      s"=== flight-pipeline " +
        s"(mode=${args.mode}, hours=${args.windowHours}, lags=${args.lags}, " +
        s"delay-threshold-min=${args.delayThresholdMinutes}, " +
        s"delay-dataset=${args.delayDataset}, feature-set=${args.featureSet}, " +
        s"sample=$sampleLabel) ==="
    )

    // --------------------------------------------------------------------
    // 3bis) Mode "report" : lecture des runs logués et export CSV
    //
    // Ce mode ne relance aucun traitement de préparation / jointure /
    // entraînement. Il se contente de relire out/metrics/train_runs
    // (produit par TrainRunLogger) et de générer :
    //   - un résumé lisible dans les logs,
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
    //
    // Référence article : sections 2.1–2.3, où la préparation des
    // données et la jointure avec la météo représentent une part
    // importante du travail (ingénierie des données).
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

      // Contrôle qualité des tables "clean" (cohérence vols/météo/aéroports)
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
    //
    // Référence : sections 2.2–2.3 de l’article (construction de Wo/Wd
    // et du "Joint Table" JT sur lequel sont définis D1..D4).
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
    //
    // Référence : sections 4.2–4.4 de l’article (construction des
    // datasets D1..D4, échantillonnage équilibré, métriques Acc/Reco/Recd).
    //
    // Les métriques détaillées (train/test) sont journalisées par
    // TrainRunLogger, puis ShowTrainingRuns construit un tableau de
    // synthèse et un export CSV.
    // --------------------------------------------------------------------
    if (args.mode == "training" || args.mode == "train" || args.mode == "all") {
      val t2 = System.currentTimeMillis()

      val trainer = new TrainRandomForest(
        spark                 = spark,
        joinIntermediatePath  = paths.joinIntermediateOut,
        outRoot               = args.outRoot,
        lags                  = args.lags,
        delayThresholdMinutes = args.delayThresholdMinutes,
        delayDatasetId        = args.delayDataset,
        featureSetId          = args.featureSet
      )

      // run() retourne un DataFrame à une seule ligne avec les métriques test
      val metricsDF  = trainer.run()
      val metricsRow = metricsDF.first()

      // Correspondance avec les noms de colonnes produits par saveTestMetrics
      val accuracy      = metricsRow.getAs[Double]("accuracy")
      val recallDelayed = metricsRow.getAs[Double]("recall_pos")   // rappel classe 1 (retards, Recd)
      val recallOnTime  = metricsRow.getAs[Double]("specificity")  // rappel classe 0 (à l'heure, Reco)

      log.info(
        f"[training] Terminé : accuracy=$accuracy%.4f, " +
          f"recallOnTime=$recallOnTime%.4f, " +
          f"recallDelayed=$recallDelayed%.4f"
      )
      log.info(s"[training] Durée: ${(System.currentTimeMillis() - t2) / 1000.0}s")

      // Mise à jour du tableau de synthèse + CSV après ce run
      ShowTrainingRuns.run(spark, args.outRoot)
    }

    // --------------------------------------------------------------------
    // 8) Arrêt propre de l'application
    // --------------------------------------------------------------------
    log.info("=== Fin flight-pipeline ===")
    spark.stop()
  }
}
