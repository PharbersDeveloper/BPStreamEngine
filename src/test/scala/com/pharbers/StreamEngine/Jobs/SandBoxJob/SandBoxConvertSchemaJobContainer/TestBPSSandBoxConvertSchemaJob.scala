package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJobContainer

import java.util.{Date, UUID}

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJob.BPSSandBoxConvertSchemaJob
import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Schema.Spark.{BPSMetaData2Map, SchemaConverter}
import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import com.pharbers.util.log.PhLogable
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.mongodb.scala.bson.ObjectId
import org.scalatest.FunSuite

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/01/15 10:42
  * @note 一些值得注意的地方
  */
class TestBPSSandBoxConvertSchemaJob extends FunSuite with PhLogable{
    test("test open and exec"){
//        BPSLocalChannel(Map())
//        val jobContainerId: String = UUID.randomUUID().toString
//        val date = new Date().getTime
//        val metaDataSavePath: String = s"/user/dcs/test/BPStreamEngine/$date/$jobContainerId/metadata"
//        val checkPointSavePath: String = s"/user/dcs/test/BPStreamEngine/$date/$jobContainerId/checkpoint"
//        val parquetSavePath: String =  s"/user/dcs/test/BPStreamEngine/$date/$jobContainerId/contents"
//
//        val jobParam = Map(
//            "parentJobId" -> "60d0e29e-af5e-4a9b-802a-1b2a6f483ee80",
//            "parentMetaData" -> "/jobs/f9b76128-8019-4d1a-bf6c-b12b683f778c/722c8bfb-00ad-49a8-9e17-404f7f952994/metadata",
//            "parentSampleData" -> "/jobs/f9b76128-8019-4d1a-bf6c-b12b683f778c/722c8bfb-00ad-49a8-9e17-404f7f952994/contents",
//            "jobContainerId" -> jobContainerId,
//            "metaDataSavePath" -> metaDataSavePath,
//            "checkPointSavePath" -> checkPointSavePath,
//            "parquetSavePath" -> parquetSavePath,
//            "dataSetId" -> new ObjectId().toString
//        )
//        val spark = BPSparkSession()
//        val convertJob: BPSSandBoxConvertSchemaJob =
//            BPSSandBoxConvertSchemaJob(
//                "test_" + UUID.randomUUID().toString,
//                jobParam,
//                spark,
//                None)
//        val metaData = spark.sparkContext.textFile(s"${jobParam("parentMetaData")}/${jobParam("parentJobId")}")
//        val primitive = BPSMetaData2Map.list2Map(metaData.collect().toList)
//        val convertContent = primitive ++ SchemaConverter.column2legalWithMetaDataSchema(primitive)
//        implicit val formats: DefaultFormats.type = DefaultFormats
//        val schemaData = write(convertContent("schema").asInstanceOf[List[Map[String, Any]]])
//        convertJob.totalRow = Some(4029864)
//        convertJob.setInputStream(SchemaConverter.str2SqlType(schemaData), None)
//        convertJob.exec()
//        ThreadExecutor.waitForShutdown()
    }
}
