package com.pharbers.StreamEngine.Jobs.MartDataCheckJob

import com.mongodb.client.{MongoClient, MongoClients, MongoCollection, MongoDatabase}
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.BPSComponentConfig
import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import org.apache.kafka.common.config.ConfigDef
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.bson.BsonDocument
import org.mongodb.scala.model.Filters

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/06/18 14:32
  * @note 一些值得注意的地方
  */
class FileSourceDataCheckJob(jobContainer: BPSJobContainer, override val componentProperty: Component2.BPComponentConfig)
        extends BPStreamJob {
    override type T = BPSCommonJobStrategy
    override val strategy: BPSCommonJobStrategy = new BPSCommonJobStrategy(componentProperty, configDef)
    override val id: String = strategy.getJobId
    override val description: String = "FileSourceDataCheckJob"
    override val spark: SparkSession = strategy.getSpark

    import spark.implicits._

    override def createConfigDef(): ConfigDef = new ConfigDef()

    override def open(): Unit = inputStream = Some(spark.sql("select * from cpa"))

    override def exec(): Unit = check()

    def check(): Unit = {
        val df = inputStream.get
        val aggCols = df.columns.filter(x => x != "TAG").map(x => first(x) as x)
        val rows = df.groupBy("TAG").agg(aggCols.head, aggCols.tail: _*).collect()
        rows.foreach(row => {
            val tag = row.getAs[String]("TAG")
            val path = getSchemaJobResPath(tag)
            println(tag)
            println(path)
            val schemaJobDf = spark.read.parquet(path).limit(1)
            val schemaJobRow = schemaJobDf.collect().head
            println("*******")
            println(df.columns.map(x => {(x, row.getAs[String](x))}).mkString(","))
            println(schemaJobDf.columns.map(x => {(x, schemaJobRow.getAs[String](x))}).mkString(","))
            println("********")
        })

    }

    def getSchemaJobResPath(tag: String): String = {
        MateDataUtil.getSchemaJobUrl(tag)
    }
}

object MateDataUtil {
    lazy val mongoClient: MongoClient = MongoClients.create("mongodb://pharbers:Pharbers.84244216@a0eb74da798af11ea934b02ff50af4f4-919388620.cn-northwest-1.elb.amazonaws.com.cn:30010")
    lazy val database: MongoDatabase = mongoClient.getDatabase("pharbers-sandbox-merge")
    lazy val jobs: MongoCollection[BsonDocument] = database.getCollection("jobs", classOf[BsonDocument])
    lazy val datasets: MongoCollection[BsonDocument] = database.getCollection("datasets", classOf[BsonDocument])
    lazy val assets: MongoCollection[BsonDocument] = database.getCollection("assets", classOf[BsonDocument])
    lazy val files: MongoCollection[BsonDocument] = database.getCollection("files", classOf[BsonDocument])

    def getSchemaJobUrl(tag: String): String = {
        val asset = assets.find(Filters.eq("martTags", tag)).first()
        val dataset = datasets.find(Filters.eq("_id", asset.getArray("dfs").get(0).asObjectId().getValue)).first()
        dataset.getString("url").getValue
    }
}

object Test extends App{
    val job = new FileSourceDataCheckJob(null, BPSComponentConfig("", "", Nil, Map()))
    job.open()
    job.exec()
}
