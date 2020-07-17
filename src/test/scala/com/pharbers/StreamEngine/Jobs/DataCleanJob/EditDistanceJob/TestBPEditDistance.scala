package com.pharbers.StreamEngine.Jobs.DataCleanJob.EditDistanceJob


import java.io.File
import java.util.{Collection, Scanner, UUID}
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.{BPSComponentConfig, BPSConcertEntry}
import com.pharbers.StreamEngine.Utils.Job.BPSJobContainer
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
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

    }

    test("test getDistance"){
        println(BPEditDistance.getDistance("天伦", "多帕菲").mkString(","))
        println(BPEditDistance.replaceFunc(BPEditDistance.getDistance("天伦", "多帕菲")))
        val a = List(Seq("1", "1", "0"), Seq("注射剂", "注射液", "1"), Seq("天伦", "多帕菲", "3"), Seq("扬子江药业集团有限公司", "齐鲁制药有限公司", "6"), Seq("0.5ml∶20mg", "20MG 0.5ML", "-1"))
        val b = List(Seq("1", "1", "0"), Seq("注射剂", "注射液", "1"), Seq("天伦", "天伦", "0"), Seq("扬子江药业集团有限公司", "扬子江药业集团有限公司", "0"), Seq("0.5ml∶20mg", "20MG 0.50ML", "8"))
        println(a.map(x => x(2).toInt).count(x => x == 0))
        println(b.map(x => x(2).toInt).count(x => x == 0))
        println(a.map(x => x(2).toInt).count(x => x == 0).compareTo(b.map(x => x(2).toInt).count(x => x == 0)))

    }

    test("filter min"){
        val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
        val df = spark.read.parquet("/user/dcs/test/tmp/distanceDf_0420")
                .filter("id == 6640019457913 and partition_id == 13")
        df.show(false)
        val bPEditDistance = new BPEditDistance(null, BPSComponentConfig("", "", Nil, Map("jobId" -> "test", "dataSets" -> "")))
        val path = s"/user/dcs/test/${UUID.randomUUID().toString}"
        bPEditDistance.filterMinDistanceRow(bPEditDistance.mappingConfig, df, path)
        spark.read.parquet(path).show(false)
        val conf = new Configuration
        val hdfs = FileSystem.get(conf)
        hdfs.delete(new Path(path), true)
        Thread.sleep(1000 * 60)
    }
}

object RunBPEditDistance extends App{
    val spark =  BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
    val id = "0511"
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