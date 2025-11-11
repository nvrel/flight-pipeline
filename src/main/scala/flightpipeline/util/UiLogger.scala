package flightpipeline.util

import org.apache.spark.sql.SparkSession

object UiLogger {
  /** Log l'URL réelle de l'UI Spark.
   * Si Spark n’a pas encore démarré l’UI, construit une URL plausible (host+port).
   */
  def logUiUrl(spark: SparkSession, prefix: String = "[ui]"): String = {
    val urlOpt = spark.sparkContext.uiWebUrl
    urlOpt match {
      case Some(url) =>
        println(s"$prefix $url")
        url
      case None =>
        println(s"$prefix UI not yet available (no uiWebUrl). Check driver logs for 'Web UI available at ...'.")
        ""
    }
  }
}