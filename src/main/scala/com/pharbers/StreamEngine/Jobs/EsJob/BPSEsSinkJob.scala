package com.pharbers.StreamEngine.Jobs.EsJob

import com.pharbers.StreamEngine.Jobs.EsJob.Listener.EsSinkJobListener
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}

object BPSEsSinkJob {
    def apply(id: String,
              spark: SparkSession,
              inputStream: Option[sql.DataFrame],
              container: BPSJobContainer,
              jobConf: Map[String, Any]): BPSEsSinkJob =
        new BPSEsSinkJob(id, spark, inputStream, container, jobConf)
}

/** 执行 EsSink 的 Job
  *
  * @author jeorch
  * @version 0.1
  * @since 2019/11/19 15:43
  * @node 可用的配置参数
  */
class BPSEsSinkJob(override val id: String,
                   override val spark: SparkSession,
                   is: Option[sql.DataFrame],
                   container: BPSJobContainer,
                   jobConf: Map[String, Any])
        extends BPStreamJob {

    type T = BPSJobStrategy
    override val strategy: BPSJobStrategy = null

    val indexName: String =jobConf.getOrElse("indexName", throw new Exception("no indexName found")).toString
    val metadata: Map[String, Any] = jobConf("metadata").asInstanceOf[Map[String, Any]]

    override def open(): Unit = {
        inputStream = is
    }

    override def exec(): Unit = {

        inputStream match {
            case Some(is) =>
                val query = is.writeStream
                    .option("checkpointLocation", "/test/jeorch/" + this.id + "/checkpoint")
                    .format("es")
                    .start(indexName)
                outputStream = query :: outputStream

                val length = metadata("length").asInstanceOf[String].tail.init.toLong
                val listener = EsSinkJobListener(id, id, spark, this, query, length)
                listener.active(null)
                listeners = listener :: listeners

            case None => ???
        }
    }

    override def close(): Unit = {
        super.close()
        container.finishJobWithId(id)
    }
}
