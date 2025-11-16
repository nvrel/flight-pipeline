package flightpipeline.eval

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._

/**
 * Calcul de métriques binaires pour la prédiction de retard,
 * en suivant la logique de l’article de Belcastro et al. (TIST 2014),
 * section "Statistics measures".
 *
 * Hypothèses de codage du label :
 *   - label = 0.0 : vol à l’heure
 *   - label = 1.0 : vol retardé (au-delà du seuil choisi)
 *
 * La matrice de confusion est lue comme dans l’article :
 *
 *                 Prédit on-time   Prédit delayed
 *  Réel on-time        TP                FN
 *  Réel delayed        FP                TN
 *
 * Acc  = (TP + TN) / total
 * Reco = TP / (TP + FN)      → rappel de la classe "à l’heure"
 * Recd = TN / (TN + FP)      → rappel de la classe "retardée"
 */
final case class BinaryClassificationMetrics(
                                              accuracy: Double,
                                              recallOnTime: Double,    // Reco dans l’article
                                              recallDelayed: Double,   // Recd dans l’article
                                              precisionDelayed: Double,
                                              f1Delayed: Double,
                                              tpOnTime: Long,
                                              fnOnTime: Long,
                                              fpDelayed: Long,
                                              tnDelayed: Long,
                                              supportOnTime: Long,
                                              supportDelayed: Long
                                            )

object BinaryClassificationMetrics {

  /** Calcule Acc / Reco / Recd + F1 et la matrice de confusion à partir d’un DataFrame de prédictions. */
  def fromPredictions(predictions: DataFrame): BinaryClassificationMetrics = {
    val spark = predictions.sparkSession
    import spark.implicits._

    // Groupement (label, prediction) → count
    val confusion = predictions
      .groupBy($"label", $"prediction")
      .agg(count(lit(1)).as("count"))
      .collect()
      .map { row =>
        val label = row.getDouble(0).toInt
        val pred  = row.getDouble(1).toInt
        val cnt   = row.getLong(2)
        ((label, pred), cnt)
      }
      .toMap

    def c(y: Int, yhat: Int): Long = confusion.getOrElse((y, yhat), 0L)

    // Notations cohérentes avec la matrice de l’article
    val c00 = c(0, 0) // on-time prédit on-time  → TP
    val c01 = c(0, 1) // on-time prédit delayed  → FN
    val c10 = c(1, 0) // delayed prédit on-time  → FP
    val c11 = c(1, 1) // delayed prédit delayed  → TN

    val total = c00 + c01 + c10 + c11

    val accuracy =
      if (total > 0L) (c00 + c11).toDouble / total.toDouble else 0.0

    // Reco : rappel de la classe "à l’heure" (ligne du haut)
    val supportOnTime = c00 + c01
    val recallOnTime =
      if (supportOnTime > 0L) c00.toDouble / supportOnTime.toDouble else 0.0

    // Recd : rappel de la classe "retardée" (ligne du bas)
    val supportDelayed = c10 + c11
    val recallDelayed =
      if (supportDelayed > 0L) c11.toDouble / supportDelayed.toDouble else 0.0

    // Précision et F1 sur la classe "retardée" (utile pour le rapport)
    val precisionDelayed =
      if (c11 + c01 > 0L) c11.toDouble / (c11 + c01).toDouble else 0.0

    val f1Delayed =
      if (precisionDelayed + recallDelayed > 0.0)
        2.0 * precisionDelayed * recallDelayed / (precisionDelayed + recallDelayed)
      else 0.0

    BinaryClassificationMetrics(
      accuracy         = accuracy,
      recallOnTime     = recallOnTime,
      recallDelayed    = recallDelayed,
      precisionDelayed = precisionDelayed,
      f1Delayed        = f1Delayed,
      tpOnTime         = c00,
      fnOnTime         = c01,
      fpDelayed        = c10,
      tnDelayed        = c11,
      supportOnTime    = supportOnTime,
      supportDelayed   = supportDelayed
    )
  }
}
