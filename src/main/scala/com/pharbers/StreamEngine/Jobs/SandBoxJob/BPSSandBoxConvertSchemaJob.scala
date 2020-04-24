package com.pharbers.StreamEngine.Jobs.SandBoxJob

import java.util.Collections

import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPJobLocalListener
import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Strategy.Blood.BPSSetBloodStrategy
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import com.pharbers.StreamEngine.Utils.Strategy.Schema.{BPSMetaData2Map, SchemaConverter}
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.msgMode.SparkQueryEvent
import com.pharbers.StreamEngine.Utils.Strategy.hdfs.BPSHDFSFile
import org.apache.kafka.common.config.ConfigDef
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.pharbers.kafka.schema.{DataSet, UploadEnd}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.mongodb.scala.bson.ObjectId

import collection.JavaConverters._

case class BPSSandBoxConvertSchemaJob(container: BPSJobContainer,
                                      componentProperty: Component2.BPComponentConfig) extends BPStreamJob {

    type T = BPSCommonJobStrategy
    override val strategy: BPSCommonJobStrategy = BPSCommonJobStrategy(componentProperty.config, configDef)
    val bloodStrategy: BPSSetBloodStrategy = new BPSSetBloodStrategy(componentProperty.config)
    override val id: String = componentProperty.id // 本身Job的id
    val jobId: String = strategy.getJobId // componentProperty config中的job Id
    val runnerId: String = BPSConcertEntry.runner_id // Runner Id
    val traceId: String = componentProperty.args.head
    val msgType: String = componentProperty.args.last
    val spark: SparkSession = strategy.getSpark
    val sc: SchemaConverter = strategy.getSchemaConverter
    val hdfs: BPSHDFSFile = strategy.getHdfsFile
    var totalNum: Long = 0
    val checkpointPath = s"/jobs/$runnerId/$id/checkpoint"
    val parquetPath = s"/jobs/$runnerId/$id/contents"
    val metaDataPath = s"/jobs/$runnerId/$id/metadata"
    val mongoId: String = new ObjectId().toString
    var metaData: MetaData = _
    implicit val formats: DefaultFormats.type = DefaultFormats

    import spark.implicits._

    override def open(): Unit = {
        inputStream = container.inputStream
        setInputStream(inputStream)
    }

    override def exec(): Unit = inputStream match {
        case Some(is) => {
            val query = startProcessParquet(is)
            outputStream = outputStream :+ query

            val rowNumListener =
                BPJobLocalListener[SparkQueryEvent](null, List(s"spark-${query.id.toString}-progress"))(x => {
                    logger.info(s"listener hit query ${x.date.id}")
                    checkQuery()
                })
            rowNumListener.active(null)
            checkQuery()
            listeners = listeners :+ rowNumListener
        }
        case None => ???
    }

    override def close(): Unit = {
        logger.info(s"Job $id ${componentProperty.name} =====> Closed")
        super.close()
        container.finishJobWithId(id)
    }

    def startProcessParquet(df: DataFrame): StreamingQuery = {
        val partitionNum = math.ceil(totalNum / 100000D).toInt
        df.filter($"jobId" === jobId and $"type" === "SandBox")
                .repartition(partitionNum)
                .writeStream
                .outputMode("append")
                .format("parquet")
                .option("checkpointLocation", checkpointPath)
                .option("path", parquetPath)
                .start()
    }

    def setInputStream(df: Option[DataFrame]): Unit = {
        //TODO 串联执行会更好,@Alex留给自己
        // 解析MetaData
        val mdPath = componentProperty.config("metaDataPath")
        metaData = startProcessMetaData(s"$mdPath/$jobId")
        totalNum = metaData.length("length").toString.toLong
        // 将规范过后的MetaData重新写入
        writeMetaData(metaDataPath, metaData)
        // 规范化的Schema设置Stream
        df match {
            case Some(is) => {
                inputStream = Some(
                    sc.column2legalWithDF("data", is)
                            .select(from_json($"data", sc.str2SqlType(write(metaData.schemaData))).as("data"))
                            .select("data.*")
                )
            }
            case None => logger.warn("Input Stream Is Nil")
        }
    }

    def startProcessMetaData(path: String): MetaData = {
        val content = spark.sparkContext.textFile(path)
        val m2m = BPSConcertEntry.queryComponentWithId("meta2map").get.asInstanceOf[BPSMetaData2Map]
        val sc = BPSConcertEntry.queryComponentWithId("schema convert").get.asInstanceOf[SchemaConverter]
        val primitive = m2m.list2Map(content.collect().toList)
        val convertContent = primitive ++ sc.column2legalWithMetaDataSchema(primitive)
        val schema = convertContent("schema").asInstanceOf[List[Map[String, Any]]]
        val label = convertContent.getOrElse("tag", Map.empty).asInstanceOf[Map[String, Any]]
        MetaData(schema, label, Map("length" -> convertContent("length").toString.toLong))
    }

    def writeMetaData(path: String, md: MetaData): Unit = {
        hdfs.appendLine2HDFS(path, write(md.schemaData))
        hdfs.appendLine2HDFS(path, write(md.label))
        hdfs.appendLine2HDFS(path, write(md.length))
    }

    def pushMsg(): Unit = {
        val pythonMetaData = PythonMetaData(mongoId, "HiveTaskNone", metaDataPath, parquetPath, s"/jobs/$runnerId")
        val dataSet = new DataSet(Collections.emptyList(),
            mongoId,
            id,
            metaData.schemaData.map(_ ("key").toString).asInstanceOf[List[CharSequence]].asJava,
            metaData.label("sheetName").toString,
            totalNum,
            parquetPath,
            "SampleData")
        val uploadEnd = new UploadEnd(mongoId, metaData.label("assetId").toString)
        // 给PythonCleanJob发送消息
        strategy.pushMsg(BPSEvents(id, traceId, msgType, pythonMetaData), isLocal = false)
        // 血缘
        bloodStrategy.pushBloodInfo(dataSet, id, traceId)
        bloodStrategy.uploadEndPoint(uploadEnd, id, traceId)
    }

    def checkQuery(): Unit = {
        val query = outputStream.head
        val cumulative = query.recentProgress.map(_.numInputRows).sum
        logger.info(s"cumulative num $cumulative, id: $id, query: ${query.id.toString}")
        if (cumulative >= totalNum) {
            pushMsg()
            this.close()
        }
    }

    override def createConfigDef(): ConfigDef = new ConfigDef()

    override val description: String = "BPSSandBoxConvertSchemaJob"

    case class MetaData(schemaData: List[Map[String, Any]], label: Map[String, Any], length: Map[String, Any])

    case class PythonMetaData(mongoId: String,
                              noticeTopic: String,
                              metadataPath: String,
                              filesPath: String,
                              resultPath: String)

}