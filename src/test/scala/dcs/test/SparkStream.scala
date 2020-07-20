package dcs.test

import java.sql.Timestamp
import java.util.{Date, UUID}

import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import collection.JavaConverters._

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
    val path = "hdfs://192.168.100.14:8020/user/dcs/test/testFileStream/source1"
    val id = UUID.randomUUID().toString
    val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession].spark
    import spark.implicits._
    spark.readStream
            .schema(StructType(Seq(StructField("value", IntegerType), StructField("timestamp", TimestampType))))
            .parquet(path)
            .select($"value", $"timestamp".cast(TimestampType))
            .withColumn("id", col("value") % 2)
            .withWatermark("timestamp", "10 minute")
            .groupBy("value").count()
            .writeStream
            .outputMode("update")
            .format("console")
            .option("checkpointLocation", s"hdfs://192.168.100.14:8020/jobs/5ea2941cb0611a4600248be8/BPSSandBoxConvertSchemaJob/${UUID.randomUUID().toString}")
            .start()
//            .start(s"hdfs://192.168.100.14:8020/jobs/5ea2941cb0611a4600248be8/BPSSandBoxConvertSchemaJob/test-${UUID.randomUUID().toString}")
    Thread.sleep(1000 * 60 * 60)
}

object write extends App{
    val spark = SparkSession.builder().config(new SparkConf().setMaster("local[2]")).enableHiveSupport().getOrCreate()
    import spark.implicits._
    List((1, new Timestamp(new Date().getTime)),(2, new Timestamp(new Date().getTime)),(3, new Timestamp(new Date().getTime)))
            .toDF("value", "timestamp")
            .write.mode("overwrite")
            .format("parquet").save("hdfs://192.168.100.14:8020/user/dcs/test/testFileStream/source1")
}

object readOssJob extends App {
    val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession].spark
    import spark.implicits._
    val outputPath = "hdfs://192.168.100.14:8020/jobs/5ea2941cb0611a4600248be8/test"
    val getCheckpointPath = "hdfs://192.168.100.14:8020/jobs/5ea2941cb0611a4600248be8/BPSSandBoxConvertSchemaJob/" + UUID.randomUUID().toString
    spark.readStream
            .option("maxFilesPerTrigger", "100")
            .schema(StructType(
                StructField("traceId", StringType) ::
                        StructField("type", StringType) ::
                        StructField("data", StringType) ::
                        StructField("timestamp", TimestampType) :: Nil
            )).parquet("s3a://ph-stream/jobs/runId_5ec37c2b67331b5e4941b7c3/BPSOssPartitionJob/jobId_bf1d317d-fa01-486e-abb4-05681de074f7/contents")
            .writeStream
            .outputMode("append")
            .foreachBatch((dataSet, _) => {
                dataSet.persist()
                dataSet.select("jobId", "data").groupBy("jobId").agg(first(schema_of_json($"data")) as "data").collect().foreach(row => {
                    val jobId = row.getAs[String]("jobId")
                    val json = row.getAs[String]("data")
                    println(json)
                    dataSet.filter($"jobId" === jobId)
//                            .select(from_json($"data", json, Map().asJava) as "data")
                            .select("data.*")
                            .write
                            .mode("append")
                            .parquet(outputPath + "/" + jobId)
                })
                dataSet.unpersist()
            })
            .option("checkpointLocation", getCheckpointPath)
            .start()

    Thread.sleep(1000 * 60 * 60)
}
