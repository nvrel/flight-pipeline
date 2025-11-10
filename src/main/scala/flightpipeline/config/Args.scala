package flightpipeline.config

final case class Args(
                       flightsDir: String,
                       weatherDir: String,
                       airportCsv: String,
                       outRoot: String,
                       windowHours: Int,
                       lags: Int,
                       mode: String,                  // "prepare" | "join" | "all" | "quality"
                       sampleMonth: Option[String]    // <-- YYYYMM si échantillon demandé
                     )

object Args {
  private val yyyyMmRx = "^[0-9]{6}$".r

  def parse(defaults: Args, raw: Array[String]): Args = {
    val asMap = raw.toList.flatMap { kv =>
      kv.split("=", 2) match {
        case Array(k, v) if k.startsWith("--") => Some(k.drop(2) -> v)
        case _                                  => None
      }
    }.toMap

    val monthOpt = asMap.get("sample-month").map(_.trim).filter {
      case yyyyMmRx() => true
      case _          => false
    }

    defaults.copy(
      flightsDir  = asMap.getOrElse("flights", defaults.flightsDir),
      weatherDir  = asMap.getOrElse("weather", defaults.weatherDir),
      airportCsv  = asMap.getOrElse("airport", defaults.airportCsv),
      outRoot     = asMap.getOrElse("out", defaults.outRoot),
      windowHours = asMap.get("hours").map(_.toInt).getOrElse(defaults.windowHours),
      lags        = asMap.get("lags").map(_.toInt).getOrElse(defaults.lags),
      mode        = asMap.getOrElse("mode", defaults.mode),
      sampleMonth = monthOpt
    )
  }
}
