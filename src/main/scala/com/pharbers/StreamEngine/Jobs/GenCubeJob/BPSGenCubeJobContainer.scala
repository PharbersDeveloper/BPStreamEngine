package com.pharbers.StreamEngine.Jobs.GenCubeJob

import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Event.BPSTypeEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPJobRemoteListener
import com.pharbers.StreamEngine.Utils.Job.BPSJobContainer
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.spark.sql.SparkSession

object BPSGenCubeJobContainer {
    def apply(componentProperty: Component2.BPComponentConfig): BPSGenCubeJobContainer =
        new BPSGenCubeJobContainer(componentProperty)
}

/** 执行 GenCube 的 Job
  *
  * @author jeorch
  * @version 0.0.1
  * @since 2020/3/19 15:43
  */
@Component(name = "BPSGenCubeJobContainer", `type` = "BPSGenCubeJobContainer")
class BPSGenCubeJobContainer(override val componentProperty: Component2.BPComponentConfig)
    extends BPSJobContainer {

    final private val HOURS_TIMER_GEN_CUBE_KEY: String = "GenCube.timer.hours"
    final private val HOURS_TIMER_GEN_CUBE_DOC: String = "Hours for a gen cube job start."
    final private val HOURS_TIMER_GEN_CUBE_DEFAULT: Long = 72

    override def createConfigDef(): ConfigDef = {
        new ConfigDef()
            .define(HOURS_TIMER_GEN_CUBE_KEY, Type.LONG, HOURS_TIMER_GEN_CUBE_DEFAULT, Importance.HIGH, HOURS_TIMER_GEN_CUBE_DOC)
    }

    val description: String = "cube_gen"
    type T = BPSCommonJobStrategy
    val strategy = BPSCommonJobStrategy(componentProperty, configDef)
    val id: String = strategy.getId
    override val spark: SparkSession = strategy.getSpark

    val timer = new java.util.Timer()
    val hours: Long = strategy.jobConfig.getLong(HOURS_TIMER_GEN_CUBE_KEY)


    override def open(): Unit = {
        logger.info("gen-cube job container open with runner-id ========>" + id)
        logger.info(s"GenCube.timer.hours =======> ${hours}")

        val task = new java.util.TimerTask {
            def run() = {
                logger.info(s"GenCube.timer start.")
                if (jobs.nonEmpty) {
                    for (job <- jobs.values) {
                        job.open()
                        job.exec()
                    }
                }
            }
        }
        timer.schedule(task, 1000L, hours * 60 * 60 * 1000L)
        logger.info(s"GenCube.timer init succeed.")
    }

    override def exec(): Unit = {
        val listenEvent = strategy.getListens
        val listener = BPJobRemoteListener[Map[String, String]](this, listenEvent.toList)(x => startJob(x))
        listener.active(null)
        listeners = listener +: listeners
    }

    override def close(): Unit = {
        timer.cancel()
        super.close()
        logger.info("gen-cube job container closed with runner-id ========>" + id)
    }

    def startJob(event: BPSTypeEvents[Map[String, String]]): Unit = {
        logger.info(s"gen-cube job container listener be triggered by traceId(${event.traceId}).")
        val job = BPSGenCubeJob(event.jobId, spark, this, event.date)
        jobs += job.id -> job
        job.open()
        job.exec()
    }

}
