// src/main/scala/flightpipeline/io/DataPaths.scala
package flightpipeline.io

final case class DataPaths(
                            flightsDir: String,
                            weatherDir: String,
                            airportCsv: String,
                            outRoot: String
                          ) {
  // Dossiers Delta intermédiaires
  val flightCleanOut: String          = s"$outRoot/flight_clean.parquet"
  val airportTimezoneCleanOut: String = s"$outRoot/airport_timezone_clean.parquet"
  val weatherCleanOut: String         = s"$outRoot/weather_clean.parquet"
  val joinIntermediateOut: String     = s"$outRoot/join_intermediate.parquet"
  def joinFlatOut(lags: Int): String  = s"$outRoot/join_flat_lag$lags.parquet"

  // Sorties liées à l'entraînement des modèles
  private val modelsRoot: String      = s"$outRoot/models"

  /** Chemin de sauvegarde du modèle Random Forest entraîné pour un couple (lags, seuil). */
  def rfModelOut(lags: Int, delayThresholdMinutes: Int): String =
    s"$modelsRoot/rf_lag${lags}_th${delayThresholdMinutes}"

  /** Chemin du rapport JSON de métriques associé au modèle Random Forest. */
  def rfMetricsOut(lags: Int, delayThresholdMinutes: Int): String =
    s"$modelsRoot/rf_lag${lags}_th${delayThresholdMinutes}_metrics.json"
}
