package flightpipeline.report

import flightpipeline.eval.TrainRunLogger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

/**
 * Restitution des expériences Random Forest journalisées par TrainRunLogger.
 *
 * - lit out/metrics/train_runs (Delta),
 * - affiche un résumé lisible dans la console,
 * - produit un export CSV détaillé pour le rapport.
 */
object ShowTrainingRuns {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Point d’entrée appelé depuis Main en mode "report".
   *
   * @param spark   SparkSession existante
   * @param outRoot répertoire racine des sorties (par ex. "out")
   */
  def run(spark: SparkSession, outRoot: String): DataFrame = {
    // 1) Lecture de l’historique des runs via TrainRunLogger
    val runs = TrainRunLogger.loadAllRuns(spark, outRoot)

    // 2) Tableau "compact" pour lecture dans la console
    val summary = runs.select(
      col("ts"),
      col("location"),
      col("num_cores"),
      col("delay_threshold_min").alias("delay_min"),
      col("lags"),
      col("window_hours"),
      col("sample_month"),
      col("dataset_id"),
      col("n_train"),
      col("n_test"),
      col("train_accuracy"),
      col("test_accuracy"),
      col("test_recall_pos").alias("test_recall_delayed"),
      col("test_specificity").alias("test_recall_on_time"),
      col("test_f1")
    )

    println()
    println("=== Historique des entraînements Random Forest (résumé) ===")
    summary.show(200, truncate = false)

    // 3) Export complet pour exploitation dans le rapport
    val exportPath = s"$outRoot/metrics/train_runs_export"

    runs
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(exportPath)

    log.info(s"[Report] Export CSV détaillé écrit dans $exportPath")

    runs
  }
}
