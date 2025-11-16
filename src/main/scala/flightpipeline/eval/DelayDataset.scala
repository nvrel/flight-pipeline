package flightpipeline.eval

/**
 * Représentation typée des jeux cibles D1..D4 décrits dans l’article
 * "Using Scalable Data Mining for Predicting Flight Delays"
 * (section 4.2, Target Data Creation, Table VI).
 *
 * D1..D4 sont imbriqués :
 *   D1 ⊂ D2 ⊂ D3 ⊂ D4
 *
 * Un identifiant court est associé à chaque jeu pour les paramètres CLI
 * et pour la table Delta de journalisation des entraînements.
 */
sealed trait DelayDataset {
  /** Identifiant stocké dans le log et manipulé en CLI. */
  def id: String
  override def toString: String = id
}

object DelayDataset {

  /** D1 : retards dus uniquement à Extreme ou NAS, ou à leur combinaison. */
  case object D1 extends DelayDataset { val id: String = "D1" }

  /** D2 : Extreme ∪ NAS dont la part NAS est ≥ seuil de retard. */
  case object D2 extends DelayDataset { val id: String = "D2" }

  /** D3 : retards où Extreme ou NAS interviennent, même avec d’autres causes. */
  case object D3 extends DelayDataset { val id: String = "D3" }

  /** D4 : tous les vols en retard (dataset de référence de l’article). */
  case object D4 extends DelayDataset { val id: String = "D4" }

  /**
   * Jeu élargi hors article : tous les vols en retard.
   * Utilisé quand aucune filtration D1..D4 n’est souhaitée.
   */
  case object DAll extends DelayDataset { val id: String = "D_all" }

  /**
   * Conversion robuste d’une chaîne CLI vers un DelayDataset.
   *
   * Exemples :
   *   "d1", "D1"        → D1
   *   "all", "D_ALL"    → DAll
   */
  def fromString(raw: String): DelayDataset =
    raw.trim.toUpperCase match {
      case "D1"    => D1
      case "D2"    => D2
      case "D3"    => D3
      case "D4"    => D4
      case "D_ALL" => DAll
      case "ALL"   => DAll
      case other =>
        throw new IllegalArgumentException(
          s"delay-dataset doit être parmi D1, D2, D3, D4, D_all (reçu: $other)"
        )
    }
}
