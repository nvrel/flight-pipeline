package flightpipeline.util

import org.apache.spark.SparkContext
import org.apache.spark.scheduler._
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.concurrent.TrieMap

/** Listener léger qui trace la progression (tâches complétées / totales) par stage. */
final class ProgressListener(sc: SparkContext, logEveryMs: Long = 2000L) extends SparkListener {
  // stageId -> (totalTasks, completed)
  private val totals    = new TrieMap[Int, Int]()
  private val completed = new TrieMap[Int, AtomicInteger]()

  @volatile private var lastLog = 0L

  override def onStageSubmitted(sse: SparkListenerStageSubmitted): Unit = {
    val st = sse.stageInfo
    totals.put(st.stageId, st.numTasks)
    completed.putIfAbsent(st.stageId, new AtomicInteger(0))
    logNow(s"[progress] submit stage ${st.stageId} '${st.name}' with ${st.numTasks} tasks")
  }

  override def onTaskEnd(t: SparkListenerTaskEnd): Unit = {
    // incrémente le compteur de la stage
    val stId = t.stageId
    completed.get(stId).foreach(_.incrementAndGet())
    maybeLog()
  }

  override def onStageCompleted(sce: SparkListenerStageCompleted): Unit = {
    val st = sce.stageInfo
    completed.put(st.stageId, new AtomicInteger(totals.getOrElse(st.stageId, st.numTasks)))
    logNow(s"[progress] completed stage ${st.stageId} '${st.name}'")
  }

  private def maybeLog(): Unit = {
    val now = System.currentTimeMillis()
    if (now - lastLog >= logEveryMs) {
      lastLog = now
      val lines = totals.keys.toSeq.sorted.map { id =>
        val tot = totals.getOrElse(id, 0)
        val done = completed.get(id).map(_.get()).getOrElse(0)
        val pct = if (tot == 0) 100.0 else (done.toDouble * 100.0 / tot)
        f"stage $id%3d: $done%6d / $tot%6d  (${pct}%.1f%%)"
      }
      if (lines.nonEmpty) {
        val ui = sc.uiWebUrl.getOrElse("")
        println("[progress]" + (if (ui.nonEmpty) s" ui spark=$ui" else ""))
        println(lines.mkString("\n"))
      }
    }
  }

  private def logNow(msg: String): Unit = {
    // on logge côté driver, via stdout (ou remplace par un logger slf4j si tu préfères)
    println(msg)
  }
}

object ProgressListener {
  def register(sc: SparkContext, logEveryMs: Long = 2000L): ProgressListener = {
    val pl = new ProgressListener(sc, logEveryMs)
    sc.addSparkListener(pl)
    pl
  }
}
