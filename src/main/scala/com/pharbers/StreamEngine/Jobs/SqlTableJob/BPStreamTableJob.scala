//package com.pharbers.StreamEngine.Jobs.SqlTableJob
//
//import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
//import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
//import com.pharbers.kafka.schema.HiveTask
//import org.apache.spark.sql.streaming.StreamingQuery
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
///** 功能描述
//  *
//  * @param args 构造参数
//  * @tparam T 构造泛型参数
//  * @author dcs
//  * @version 0.0
//  * @since 2019/12/11 11:42
//  * @note 一些值得注意的地方
//  */
//case class BPStreamTableJob(jobContainer: BPSJobContainer, jobId: String, runId: String, spark: SparkSession, msg: HiveTask) extends BPStreamJob {
//    override type T = BPSJobStrategy
//    override val strategy: BPSJobStrategy = null
//    override val id: String = jobId
//
//    val url = ""
//    val saveMode = "append"
//    val tableName = "cpa"
//    val version = "0.0.0"
//
//    override def open(): Unit = {
//        val df = spark.readStream.csv(url)
//        inputStream = Some(df)
//    }
//
//    override def exec(): Unit = {
//        inputStream match {
//            case Some(df) => outputStream = outputStream :+ saveAsParquet(df)
//            case _ =>
//        }
//    }
//
//    def saveAsParquet(df: DataFrame): StreamingQuery = {
//        df.writeStream
//                .outputMode("append")
//                .format("parquet")
//                .option("checkpointLocation", "/jobs/" + runId + "/checkpoint/" + jobId)
//                .option("path", s"/common/public/$tableName/$version/$jobId")
//                .start()
//    }
//
//    override def close(): Unit = {
//        super.close()
//        jobContainer.finishJobWithId(id)
//    }
//}
