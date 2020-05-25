package com.pharbers.StreamEngine.Jobs.PyPipeJob2

import java.util.{Collections, UUID}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.pharbers.StreamEngine.Jobs.PyPipeJob2.Listener.BPSPipeProgressListenerAndClose
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Event.StreamListener.{BPJobLocalListener, BPStreamListener}
import com.pharbers.StreamEngine.Utils.Job.BPStreamJob
import com.pharbers.StreamEngine.Utils.Job.Status.BPSJobStatus
import com.pharbers.StreamEngine.Utils.Module.bloodModules.{BloodModel, BloodModel2}
import com.pharbers.StreamEngine.Utils.Strategy.BPStrategyComponent
import com.pharbers.StreamEngine.Utils.Strategy.Blood.BPSSetBloodStrategy
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.msgMode.SparkQueryEvent
import com.pharbers.StreamEngine.Utils.Strategy.hdfs.BPSHDFSFile
import com.pharbers.kafka.schema.DataSet
import org.apache.kafka.common.config.ConfigDef
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object BPSPythonPipeJob {
    def apply(jobId: String,
              spark: SparkSession,
              inputStream: Option[sql.DataFrame],
              noticeFunc: (String, Map[String, Any]) => Unit,
              jobCloseFunc: String => Unit,
              jobConf: Map[String, Any]): BPSPythonPipeJob =
        new BPSPythonPipeJob(jobId, spark, inputStream, noticeFunc, jobCloseFunc, jobConf)
}

/** 执行 Python 的 Job
 *
 * @author clock
 * @version 0.0.1
 * @since 2019/11/6 17:43
 * @node jobConf 可用的配置参数
 * {{{
 *     noticeTopic = "noticeTopic" // job完成后的通知位置
 *
 *     datasetId = ObjectId  // 血统中的唯一标识符
 *     parentsId = ObjectId // 上一步的 datasetId
 *
 *     resultPath = "./jobs/runId/containerId/" // Job 执行后的结果的存放位置, 会自动添加 jobId
 *     lastMetadata = Map("jobId" -> "a", "fileName" -> "b") // 上一步的元数据信息
 *
 *     fileSuffix = "csv" // 存放文件的后缀名
 *     partition = "4" //表示每个 Job 可使用的 spark 分区数，也是可用 Python 的线程数
 *     retryCount = "3" // Job 失败的重试次数
 * }}}
 */
class BPSPythonPipeJob(override val jobId: String,
                       override val spark: SparkSession,
                       is: Option[sql.DataFrame],
                       noticeFunc: (String, Map[String, Any]) => Unit,
                       jobCloseFunc: String => Unit,
                       jobConf: Map[String, Any]) extends BPStreamJob with Serializable {
    
    type T = BPStrategyComponent
    override val strategy: BPStrategyComponent = null
    override val id: String = UUID.randomUUID().toString
    override val description: String = "BPSPyCleanJob"
    val bloodStrategy: BPSSetBloodStrategy = new BPSSetBloodStrategy(Map.empty)

    val noticeTopic: String = jobConf("noticeTopic").toString
    val datasetId: String = jobConf("datasetId").toString
    val parentsId: List[String] = jobConf("parentsId").asInstanceOf[List[String]]
    val assetId: String = jobConf("assetId").toString

//    val resultPath: String = {
//        val path = jobConf("resultPath").toString
//        if (path.endsWith("/")) path + id
//        else path + "/" + id
//    }
    val lastMetadata: Map[String, Any] = jobConf("lastMetadata").asInstanceOf[Map[String, Any]]
    val data_length: Long = lastMetadata("length").asInstanceOf[Double].toLong

    val fileSuffix: String = jobConf("fileSuffix").toString
    val partition: Int = jobConf("partition").asInstanceOf[String].toInt
    val retryCount: String = jobConf("retryCount").toString

    val checkpointPath: String = getCheckpointPath
//    val rowRecordPath: String = resultPath + "/row_record"
//    val metadataPath: String = resultPath + "/metadata"
    val successPath: String = getOutputPath
    val errPath: String = "s3a://ph-stream/jobs/" + s"runId_${BPSConcertEntry.runner_id}" + "/" + description + "/" + s"jobId_$jobId" + "/" + s"id_$id" + "/err"

    import spark.implicits._

    override def open(): Unit = {
        regPedigree(BPSJobStatus.Start.toString)
        inputStream = is
    }

    override def exec(): Unit = {
        inputStream match {
            case Some(is) =>
                val mapper = new ObjectMapper()
                mapper.registerModule(DefaultScalaModule)
                val schema = StructType(StructField("tag", IntegerType) ::
                        StructField("data", StringType) ::
                        StructField("errMsg", StringType) ::
                        StructField("metadata", StringType) :: Nil
                )
                val query = is
                    .writeStream
                    .option("checkpointLocation", checkpointPath)
                    .foreachBatch((batchDF, _) => {
                        batchDF.persist()
                        val pythonDf = batchDF.select(to_json(struct($"*")).as("data"),
                                       lit(mapper.writeValueAsString(lastMetadata)).as("metadata"))
                                .select(to_json(struct($"data".as("data"), $"metadata".as("metadata"))))
//                                .rdd.pipe("python3 /Users/qianpeng/GitHub/BPStreamEngine/BPSPythonPipeJobContainer/main.py")
                                .rdd.pipe("python3 ./main.py")
                                .toDF("data")
                                .select(from_json($"data", schema) as "data")
                                .select("data.*")
                                .cache()
                        pythonDf.filter("tag == 1")
                                        .select("data")
                                        .write.text(successPath)
                        pythonDf.filter("tag != 1")
                                .select("errMsg")
                                .write.text(errPath)
                        pythonDf.unpersist()
                        batchDF.unpersist()
                    })
                    .start()

                outputStream = query :: outputStream
                val stopListener = BPJobLocalListener[SparkQueryEvent](null, List(s"spark-${query.id.toString}-progress"))(x => {
                    logger.info(s"listener hit python job query ${x.data.id}")
                    val cumulative = query.recentProgress.map(_.numInputRows).sum
                    logger.info(s"cumulative num $cumulative, id: $id, query: ${query.id.toString}")
                    if (cumulative >= data_length) {
                        logger.info(s"python query: ${query.id.toString} over")
                        this.close()
                    }
                })
                listeners = listeners ::: stopListener :: Nil
                stopListener.active(null)

            case None => ???
        }
    }

    // 注册血统
    def regPedigree(status: String): Unit = {
//        val dfs = BloodModel(
//            datasetId,
//            assetId,
//            parentsId,
//            id,
//            Nil,
//            "",
//            data_length,
//            successPath,
//            "Python 清洗 Job", status)
        val dfs = BloodModel2(
            jobId = jobId,
            columnNames = Nil,
            tabName = "",
            length = data_length,
            url = successPath,
            description = "pyJob",
            status = status
        )
        // TODO 齐 弄出traceId
        bloodStrategy.pushBloodInfo(dfs, jobId,"")
    }

    override def close(): Unit = {
        regPedigree(BPSJobStatus.End.toString)
        noticeFunc(noticeTopic, Map(
            "jobId" -> id,
            "datasetId" -> datasetId,
            "length" -> data_length,
//            "resultPath" -> resultPath,
//            "rowRecordPath" -> rowRecordPath,
//            "metadataPath" -> metadataPath,
            "successPath" -> successPath,
            "errPath" -> errPath
        ))
        super.close()
        jobCloseFunc(id)
    }

    override val componentProperty: Component2.BPComponentConfig = null

    override def createConfigDef(): ConfigDef = ???
}
