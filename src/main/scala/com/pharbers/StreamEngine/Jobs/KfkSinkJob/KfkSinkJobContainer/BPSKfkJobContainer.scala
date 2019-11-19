package com.pharbers.StreamEngine.Jobs.KfkSinkJob.KfkSinkJobContainer

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.KfkSinkJob.BPSKfkSinkJob
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import org.apache.spark.sql.SparkSession

object BPSKfkJobContainer {
    // TODO: concertJob 在初始化过程中随着container一起启动的Job 实例
    def apply(strategy: BPSKfkJobStrategy, spark: SparkSession, concertJobs: List[Map[String, Any]]): BPSKfkJobContainer = new BPSKfkJobContainer(strategy, spark, concertJobs)
}

class BPSKfkJobContainer(override val strategy: BPSKfkJobStrategy, val spark: SparkSession, concertJobs: List[Map[String, Any]]) extends BPSJobContainer {
    override val id: String = UUID.randomUUID().toString
    type T = BPSKfkJobStrategy
    import spark.implicits._

    override def open(): Unit = {
        // TODO: 入流的来源, 只有两种
        //      1. 对文件流的读取
        //      2. 对外部流的接入
        // 这里需要向下抽象, 抽象出一层入流的接收逻辑
    }

    override def exec(): Unit = inputStream match {
        case Some(is) => {
            concertJobs foreach { concert =>
//                val concert = Map("topic" -> "alfred-test")
                val job = BPSKfkSinkJob(id, spark, this.inputStream, this, this.strategy, concert)
                job.exec()
                jobs += id -> job
                job
            }
        }
        case None => ???
    }

    override def getJobWithId(id: String, category: String = ""): BPStreamJob = {
        jobs.get(id) match {
            case Some(job) => job
            case None => {
                val concert = Map("topic" -> "alfred-test")
                val job = BPSKfkSinkJob(id, spark, this.inputStream, this, this.strategy, concert)
                job.exec()
                jobs += id -> job
                job
            }
        }
    }
}
