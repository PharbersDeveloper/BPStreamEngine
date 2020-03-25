package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJob

import java.util.{Collections, UUID}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import com.pharbers.StreamEngine.Jobs.SandBoxJob.BloodJob.BPSBloodJob
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJob.Listener.ConvertSchemaListener
import com.pharbers.StreamEngine.Jobs.SandBoxJob.UploadEndJob.BPSUploadEndJob
import com.pharbers.StreamEngine.Utils.Component.Dynamic.JobMsg
import com.pharbers.StreamEngine.Utils.HDFS.BPSHDFSFile
import com.pharbers.StreamEngine.Utils.Schema.Spark.{BPSMetaData2Map, SchemaConverter}
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.{BPJob, DataSet, UploadEnd}
import org.apache.avro.specific.SpecificRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._

import collection.JavaConverters._

object BPSSandBoxConvertSchemaJob {
    def apply(id: String,
              jobParam: Map[String, String],
              spark: SparkSession,
              df: Option[DataFrame],
              execQueueJob: AtomicInteger): BPSSandBoxConvertSchemaJob =
        new BPSSandBoxConvertSchemaJob(id, jobParam, spark, df, execQueueJob)
}

class BPSSandBoxConvertSchemaJob(val id: String,
                                 jobParam: Map[String, String],
                                 val spark: SparkSession,
                                 df: Option[DataFrame],
                                 execQueueJob: AtomicInteger) extends BPSJobContainer {

    type T = BPSKfkJobStrategy
    val strategy: Null = null

    import spark.implicits._

    // TODO: 想个办法把这个东西搞出去
    var totalRow: Option[Long] = None
    var columnNames: List[CharSequence] = Nil
    var sheetName: Option[String] = None
    var dataAssetId: Option[String] = None

    override def open(): Unit = {
        val metaData = spark.sparkContext.textFile(s"${jobParam("parentMetaData")}/${jobParam("parentJobId")}")
        val (schemaData, colNames, tabName, length, assetId) =
            writeMetaData(metaData, s"${jobParam("metaDataSavePath")}")
        totalRow = Some(length)
        columnNames = colNames
        sheetName = Some(tabName)
        dataAssetId = Some(assetId)

        if (schemaData.isEmpty || assetId.isEmpty) {
            // TODO: metadata中缺少schema 和 asset标识，走错误流程
            logger.error("Schema Is Null")
            logger.error(s"AssetId Is Null ====> $assetId, Path ====> ${jobParam("parquetSavePath")}")
            logger.error(s"AssetId Is Null ====> $assetId, Path ====> ${jobParam("metaDataSavePath")}")
            this.close()
        } else {
            val schema = SchemaConverter.str2SqlType(schemaData)
            logger.info(s"ParentSampleData Info ${jobParam("parentSampleData")}")
            setInputStream(schema, df)
    
            // TODO: 临时
            BPSHDFSFile.createPath(jobParam("parquetSavePath"))
            
            pushPyJob(Map(
                "parentsId" -> (jobParam("dataSetId") :: Nil).mkString(","),
                "noticeTopic" -> "HiveTaskNone",
                "metadataPath" -> jobParam("metaDataSavePath"),
                "filesPath" -> jobParam("parquetSavePath"),
                "resultPath" -> s"/jobs/$id"
            ))
            
        }
    }

    override def exec(): Unit = {
        inputStream match {
            case Some(is) =>
                val query = is.filter($"jobId" === jobParam("parentJobId") and $"type" === "SandBox")
                        .writeStream
                        .outputMode("append")
                        .format("parquet")
                        .option("checkpointLocation", jobParam("checkPointSavePath"))
                        .option("path", s"${jobParam("parquetSavePath")}")
                        .start()

                logger.debug(s"Parquet Save Path Your Path =======> ${jobParam("parquetSavePath")}")

                outputStream = query :: outputStream
                val listener = ConvertSchemaListener(id, jobParam("parentJobId"), spark, this, query, totalRow.get)
                listener.active(null)
                listeners = listener :: listeners
            case None => logger.warn("Stream Is Null")
        }
    }

    override def close(): Unit = {
    // TODO 将处理好的Schema发送邮件
        BPSBloodJob(
            "data_set_job",
            new DataSet(
                Collections.emptyList(),
                jobParam("dataSetId"),
                jobParam("jobContainerId"),
                columnNames.asJava,
                sheetName.get,
                totalRow.get,
                s"${jobParam("parquetSavePath")}",
                "SampleData")).exec()

        val uploadEnd = new UploadEnd(jobParam("dataSetId"), dataAssetId.get)
        BPSUploadEndJob("upload_end_job", uploadEnd).exec()
    
        execQueueJob.decrementAndGet()
        totalRow = None
        columnNames = Nil
        sheetName = None
        dataAssetId = None
        logger.debug(s"Self Close Job With ID == =====>${id}")
        
        super.close()
        outputStream.foreach(_.stop())
        listeners.foreach(_.deActive())
    
        
    }

    def writeMetaData(metaData: RDD[String], path: String): (String, List[CharSequence], String, Long, String) = {
        try {
            val primitive = BPSMetaData2Map.list2Map(metaData.collect().toList)
            val convertContent = primitive ++ SchemaConverter.column2legalWithMetaDataSchema(primitive)

            implicit val formats: DefaultFormats.type = DefaultFormats
            val schema = write(convertContent("schema").asInstanceOf[List[Map[String, Any]]])
            val colNames = convertContent("schema").asInstanceOf[List[Map[String, Any]]].map(_ ("key").toString)
            val tabName = convertContent.
                    getOrElse("tag", Map.empty).
                    asInstanceOf[Map[String, Any]].
                    getOrElse("sheetName", "").toString

            val assetId = convertContent.
                    getOrElse("tag", Map.empty).
                    asInstanceOf[Map[String, Any]].
                    getOrElse("assetId", "").toString
            // TODO: 这块儿还要改进
            convertContent.foreach { x =>
                if (x._1 == "length") {
                    BPSHDFSFile.appendLine2HDFS(path, s"""{"length": ${x._2}}""")
                } else {
                    BPSHDFSFile.appendLine2HDFS(path, write(x._2))
                }
            }
            (schema, colNames, tabName, convertContent("length").toString.toLong, assetId)
        } catch {
            case e: Exception =>
                // TODO: 处理不了发送重试
                logger.error(e.getMessage)
                ("", Nil, "", 0, "")
        }

    }

    def setInputStream(schema: DataType, df: Option[DataFrame]): Unit = {
         df match {
            case Some(reading) =>
                reading.filter($"jobId" === jobParam("parentJobId") and $"type" === "SandBox")
                inputStream = Some(
                    SchemaConverter.column2legalWithDF("data", reading)
                        .select(from_json($"data", schema).as("data"))
                        .select("data.*")
                )
            case None => logger.info("reading is none")
        }
    }
    
    private def pushPyJob(job: Map[String, String]) {
        implicit val formats: DefaultFormats.type = DefaultFormats
        val jobMsg = write(job)
        val topic = "PyJobContainerListenerTopic"
        
        val pkp = new PharbersKafkaProducer[String, BPJob]
        val bpJob = new BPJob("", "", "", jobMsg)
        val fu = pkp.produce(topic, id, bpJob)
        println(fu.get(10, TimeUnit.SECONDS))
        pkp.producer.close()
    }
}
