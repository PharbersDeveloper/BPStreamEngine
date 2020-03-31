package com.pharbers.StreamEngine.Jobs.EditDistanceJob

import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/03/19 15:14
  * @note 一些值得注意的地方
  */
class TestBPEditDistance extends FunSuite{
    test("test run"){
        val spark = BPSparkSession()
        val config = Map("runId" -> "0331", "jobId" -> "0331", "dataSets" -> "")
        val jobContainer = new BPSJobContainer() {
            override type T = BPSJobStrategy
            override val strategy: BPSJobStrategy = null
            override val id: String = ""
            override val spark: SparkSession = spark
        }
        jobContainer.inputStream = Some(spark.sql("select * from cpa"))
        val bPEditDistance = BPEditDistance(jobContainer, spark, config)
        bPEditDistance.open()
        bPEditDistance.exec()
    }
}
