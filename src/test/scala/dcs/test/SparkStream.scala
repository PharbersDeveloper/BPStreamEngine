package dcs.test

import java.util.UUID

import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/05/11 15:07
  * @note 一些值得注意的地方
  */
object SparkStream extends App {
    val path = "hdfs://192.168.100.14:8020/user/dcs/test/testFileStream/source"
    val id = UUID.randomUUID().toString
    val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
    spark.readStream
            .schema(StructType(Seq(StructField("value", IntegerType))))
            .parquet(path)
            .writeStream
            .foreachBatch((data, batchId) => {
                data.select("value").rdd
                        .map(x => x.getAs[Int](0))
                        .pipe("python D:\\code\\pharbers\\BPStream\\BPStreamEngine\\src\\py\\test.py")
                        .saveAsTextFile("hdfs://192.168.100.14:8020/user/dcs/test/testFileStream/res9")
            })
            .option("checkpointLocation", "hdfs://192.168.100.14:8020/jobs/5ea2941cb0611a4600248be8/BPSSandBoxConvertSchemaJob/00032cf1-de76-4c00-9de7-5445f815d3ba/test")
            .start()
    Thread.sleep(1000 * 60 * 60)
}

object write extends App{
    val spark = SparkSession.builder().config(new SparkConf().setMaster("local[2]")).enableHiveSupport().getOrCreate()
    import spark.implicits._
    List(1,2,3).toDF("value")
            .write.mode("append")
            .format("delta").save("s3a://ph-stream/test/test_delta")
}
