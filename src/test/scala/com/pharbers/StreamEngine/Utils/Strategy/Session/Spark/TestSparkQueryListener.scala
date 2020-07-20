package com.pharbers.StreamEngine.Utils.Strategy.Session.Spark

import java.net.InetAddress
import java.util.UUID

import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPJobLocalListener
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.msgMode.SparkQueryEvent
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/04/09 15:22
  * @note 一些值得注意的地方
  */
class TestSparkQueryListener extends FunSuite with BeforeAndAfterAll {
    val user: String = System.getenv().get("USERNAME")
    val file = "/test/TMTest/inputParquet/cal_data"
    val check = s"/user/$user/temp/check/${UUID.randomUUID().toString}"
    val out = s"/user/$user/temp/file/${UUID.randomUUID().toString}"
    val spark: SparkSession = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
    spark.sparkContext.setLogLevel("WARN")
    test("test listen spark query") {
        val schema = spark.read.parquet(file).schema
        val query = spark.readStream
                .schema(schema)
                .parquet(file)
                .writeStream.format("parquet")
                .option("checkpointLocation", check)
                .start(out)
        val startListener = BPJobLocalListener[SparkQueryEvent](null, List(s"spark-${query.id.toString}-start"))(x =>
            assert(x.data.id == query.id.toString && x.data.status == "start"))
        startListener.active(null)
        val progressListener = BPJobLocalListener[SparkQueryEvent](null, List(s"spark-${query.id.toString}-progress"))(x =>
            assert(x.data.id == query.id.toString && x.data.status == "progress"))
        progressListener.active(null)

        val terminatedListener = BPJobLocalListener[SparkQueryEvent](null, List(s"spark-${query.id.toString}-terminated"))(x =>
            assert(x.data.id == query.id.toString && x.data.status == "terminated"))
        terminatedListener.active(null)

        //listen rowNum的例子
        val sumRow = 10
        val rowNumListener = BPJobLocalListener[SparkQueryEvent](null, List(s"spark-${query.id.toString}-progress"))(_ => {
            val cumulative = query.recentProgress.map(_.numInputRows).sum
            println(s"cumulative num $cumulative")
            if (cumulative >= sumRow) {
                println("job close")
            }
        })
        rowNumListener.active(null)
        Thread.sleep(1000 * 30)
        query.stop()
    }

    override def afterAll(): Unit = {
        spark.close()
        val conf = new Configuration
        val hdfs = FileSystem.get(conf)
        hdfs.delete(new Path(out), true)
        hdfs.delete(new Path(check), true)
        hdfs.close()
    }
}
