package flightpipeline.config

/**
 * Paramètres d’exécution de l’application.
 *
 * La plupart des champs reprennent la configuration par défaut définie
 * dans application.conf, mais peuvent être surchargés par la ligne de
 * commande (voir `parse` plus bas).
 *
 * Nouveau paramètre :
 *   - delayDataset : identifiant du dataset de retards ciblé (D1..D4),
 *     au sens de la section 4.2 de Belcastro et al. (définition de D1–D4).
 */
final case class Args(
                       flightsDir: String,
                       weatherDir: String,
                       airportCsv: String,
                       outRoot: String,
                       windowHours: Int,
                       lags: Int,
                       delayThresholdMinutes: Int,
                       mode: String,
                       sampleMonth: Option[String],
                       delayDataset: String            // "D1", "D2", "D3", "D4" ou "D_all"
                     )

object Args {

  // Format YYYYMM utilisé pour restreindre le périmètre à un mois donné.
  private val MonthPattern = "^[0-9]{6}$".r

  /**
   * Parsing très simple de la ligne de commande sous la forme :
   *   --clé valeur
   *
   * Exemple :
   *   --mode training --lags 7 --delay-threshold-min 60 --delay-dataset D3
   *
   * Les valeurs non fournies conservent celles de `defaults`.
   */
  def parse(defaults: Args, raw: Array[String]): Args = {

    // Conversion des arguments en dictionnaire clé → valeur.
    // Si un argument apparaît plusieurs fois, la dernière occurrence gagne.
    val cli: Map[String, String] =
      raw.sliding(2, 2).collect {
        case Array(k, v) if k.startsWith("--") => k.stripPrefix("--") -> v
      }.toMap

    def intOpt(key: String): Option[Int] =
      cli.get(key).map(_.toInt)

    val flightsDir = cli.getOrElse("flights", defaults.flightsDir)
    val weatherDir = cli.getOrElse("weather", defaults.weatherDir)
    val airportCsv = cli.getOrElse("airport", defaults.airportCsv)
    val outRoot    = cli.getOrElse("out", defaults.outRoot)

    val windowHours = intOpt("hours").getOrElse(defaults.windowHours)
    val lags        = intOpt("lags").getOrElse(defaults.lags)

    // On tolère --delay-threshold et --delay-threshold-min
    val delayThMin =
      cli.get("delay-threshold-min")
        .orElse(cli.get("delay-threshold"))
        .map(_.toInt)
        .getOrElse(defaults.delayThresholdMinutes)

    val mode = cli.getOrElse("mode", defaults.mode)

    val sampleMonth: Option[String] = cli.get("sample-month") match {
      case None =>
        defaults.sampleMonth

      case Some(MonthPattern(m)) =>
        Some(m)

      case Some(other) =>
        throw new IllegalArgumentException(
          s"--sample-month doit être au format YYYYMM (ex : 201205). " +
            s"Valeur reçue : $other"
        )
    }

    // Dataset ciblé : D1, D2, D3, D4 ou ALL/D_all.
    // Référence : Belcastro et al., TIST 2014, section 4.2 (datasets D1–D4).
    val delayDataset: String = {
      val rawValue =
        cli.get("delay-dataset")
          .getOrElse(defaults.delayDataset)

      rawValue.toUpperCase match {
        case "D1" | "D2" | "D3" | "D4" =>
          rawValue.toUpperCase
        case "ALL" | "D_ALL" =>
          "D_all"   // valeur canonique utilisée dans la journalisation
        case other =>
          throw new IllegalArgumentException(
            s"--delay-dataset doit être D1, D2, D3, D4 ou ALL. Valeur reçue : $other"
          )
      }
    }

    defaults.copy(
      flightsDir            = flightsDir,
      weatherDir            = weatherDir,
      airportCsv            = airportCsv,
      outRoot               = outRoot,
      windowHours           = windowHours,
      lags                  = lags,
      delayThresholdMinutes = delayThMin,
      mode                  = mode,
      sampleMonth           = sampleMonth,
      delayDataset          = delayDataset
    )
  }
}
