package com.pharbers.StreamEngine.Jobs.EditDistanceJob


import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.{BPSComponentConfig, BPSConcertEntry}
import com.pharbers.StreamEngine.Utils.Job.BPSJobContainer
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.kafka.common.config.ConfigDef
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
        val spark =  BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
        val id = "0413"
        val config = Map("jobId" -> id, "dataSets" -> "")
        val jobContainer = new BPSJobContainer() {
            override type T = BPSCommonJobStrategy
            override val strategy: BPSCommonJobStrategy = null
            override val id: String = ""
            override val description: String = ""
            override val spark: SparkSession = spark
            override val componentProperty: Component2.BPComponentConfig = null

            override def createConfigDef(): ConfigDef = ???
        }
        jobContainer.inputStream = Some(spark.sql("select * from cpa"))
        val bPEditDistance = new BPEditDistance(jobContainer, BPSComponentConfig(id, "", Nil, config))
        bPEditDistance.open()
        bPEditDistance.exec()
    }
}
