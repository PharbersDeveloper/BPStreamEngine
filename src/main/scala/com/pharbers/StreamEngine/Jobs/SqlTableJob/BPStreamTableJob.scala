package com.pharbers.StreamEngine.Jobs.SqlTableJob

import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/12/11 11:42
  * @note 一些值得注意的地方
  */
//todo: 等协议定下来后，更改msg类型
case class BPSqlTableJob(jobContainer: BPSJobContainer, jobId: String, runId: String, spark: SparkSession, msg: Any) extends BPStreamJob {
    override type T = this.type
    override val strategy: BPSqlTableJob.this.type = null
    override val id: String = jobId

    val url = ""
    val saveMode = "append"
    val tableName = "cpa"
    val version = "0.0.0"

    override def open(): Unit = {
        val df = spark.readStream.csv(url)
        inputStream = Some(df)
    }

    override def exec(): Unit = {
        inputStream match {
            case Some(df) =>
            case _ =>
        }
    }

    def saveAsParquet(df: DataFrame): Unit = {
        df.writeStream
                .partitionBy("YEAR", "MONTH")
                .outputMode("append")
                .format("parquet")
                .option("checkpointLocation", "/jobs/" + runId + "/checkpoint/" + jobId)
                .option("path", s"/common/public/$tableName/$version")
                .start()
    }

    override def close(): Unit = {
        super.close()
        jobContainer.finishJobWithId(id)
    }
}
