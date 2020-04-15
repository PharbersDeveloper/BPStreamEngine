package com.pharbers.StreamEngine.Utils.Strategy.Session.Spark

import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.msgMode.SparkQueryEvent
import com.pharbers.util.log.PhLogable
import org.apache.spark.scheduler._
import org.apache.spark.sql.streaming.StreamingQueryListener

/** spark流查询监听器
  *
  * @author dcs
  * @version 0.0
  * @since 2019/11/14 10:24
  * @note 一些值得注意的地方
  */
class SparkQueryListener extends StreamingQueryListener with PhLogable {
    logger.info("初始化SparkQueryListener")
    val localChanel: BPSLocalChannel = BPSConcertEntry.queryComponentWithId("local channel").get.asInstanceOf[BPSLocalChannel]


    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        logger.debug(s"listener:${event.id},${event.name},${event.runId}")
        val bpsEvents = BPSEvents(
            event.runId.toString,
            "",
            s"spark-${event.id}-start",
            SparkQueryEvent(event.id.toString, event.runId.toString, "start", "start")
        )
        localChanel.offer(bpsEvents)
    }

    override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        val progressJson = event.progress.prettyJson
        if(event.progress.numInputRows > 0) {
            logger.info(progressJson)
            val bpsEvents = BPSEvents(
                event.progress.runId.toString,
                "",
                s"spark-${event.progress.id}-progress",
                SparkQueryEvent(event.progress.id.toString, event.progress.runId.toString, "progress", progressJson)
            )
            localChanel.offer(bpsEvents)
        }
    }

    override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        logger.debug(s"listener:${event.id},${event.exception.getOrElse("null")},${event.runId}")
        event.exception match {
            case Some(e) => logger.error(s"listener:${event.id},$e,${event.runId}")
            case _ => logger.debug(s"listener:${event.id},${event.runId}")
        }
        val bpsEvents = BPSEvents(
            event.runId.toString,
            "",
            s"spark-${event.id}-terminated",
            SparkQueryEvent(event.id.toString, event.runId.toString, "terminated", event.exception.getOrElse("normal termination"))
        )
        localChanel.offer(bpsEvents)
    }
}

class BPSparkListener extends SparkListener with PhLogable {
    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        logger.debug("job start" + jobStart.jobId)
    }

    override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        logger.debug("task start" + taskStart.stageId)
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        logger.debug("onTaskEnd" + taskEnd.stageId)
    }

    override def onOtherEvent(event: SparkListenerEvent): Unit = {
        logger.debug("other event" + event.toString)
    }

    override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
        logger.debug("onExecutorAdded" + executorAdded.executorId + executorAdded.executorInfo)
    }

}
