// src/main/scala/flightpipeline/io/DataPaths.scala
package flightpipeline.io

final case class DataPaths(
                            flightsDir: String,
                            weatherDir: String,
                            airportCsv: String,
                            outRoot: String
                          ) {
  val flightCleanOut: String          = s"$outRoot/flight_clean.parquet"
  val airportTimezoneCleanOut: String = s"$outRoot/airport_timezone_clean.parquet"
  val weatherCleanOut: String         = s"$outRoot/weather_clean.parquet"
  val joinIntermediateOut: String     = s"$outRoot/join_intermediate.parquet"
  def joinFlatOut(lags: Int): String  = s"$outRoot/join_flat_lag$lags.parquet"
}
