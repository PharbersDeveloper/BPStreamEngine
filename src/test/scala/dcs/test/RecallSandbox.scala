package dcs.test

import java.io.File
import java.util.{Scanner, UUID}
import java.util.concurrent.TimeUnit

import com.mongodb.client.MongoClients
import com.mongodb.client.model.Filters
import com.pharbers.StreamEngine.Utils.Component.Dynamic.JobMsg
import com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.PharbersKafkaProducer
import com.pharbers.kafka.schema.BPJob
import org.apache.spark.sql.SparkSession
import org.bson.{BsonArray, BsonDocument, Document}
import org.mongodb.scala.bson.ObjectId

import collection.JavaConverters._

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/12/17 10:31
  * @note 一些值得注意的地方
  */
object RecallSandbox extends App {
    val runId = UUID.randomUUID().toString
    val mongoClient = MongoClients.create("mongodb://123.56.179.133:5555")
    val database = mongoClient.getDatabase("pharbers-sandbox-600")

    val assets = database.getCollection("assets", classOf[BsonDocument])
    val datasets = database.getCollection("datasets", classOf[BsonDocument])
    val companys = "Astellas" ::
            "Servier" ::
            "Sankyo" ::
            Nil

    val assetsIterator = assets.find(Document.parse("""{"providers": {"$in":["Lilly", "Servier", "Bayer", "Lilly"]}}""")).iterator()
    while (assetsIterator.hasNext) {
        val asset = assetsIterator.next()
        val iterator = asset.getArray("dfs").iterator()
//        while (iterator.hasNext) {
//            val id = iterator.next().asObjectId()
//            val url = datasets.find(Filters.eq(id)).first().getString("url").getValue
//            pushJob(url, id.getValue.toString, runId)
//        }

    }

    def pushJob(url: String, id: String, runId: String): Unit = {
        pushPyjob(
            runId,
            url.replace("contents", "metadata"),
            url,
            UUID.randomUUID().toString,
            id
        )
    }

    private def pushPyjob(runId: String,
                          metadataPath: String,
                          filesPath: String,
                          parentJobId: String,
                          dsIds: String): Unit = {
        val resultPath = s"hdfs:///user/alex/jobs/$runId/${UUID.randomUUID().toString}/contents"
        import org.json4s._
        import org.json4s.jackson.Serialization.write
        implicit val formats: DefaultFormats.type = DefaultFormats
        val traceId = ""
        val `type` = "add"
        val jobConfig = Map(
            "jobId" -> parentJobId,
            "parentsOId" -> dsIds,
            "metadataPath" -> metadataPath,
            "filesPath" -> filesPath,
            "resultPath" -> resultPath
        )
        val job = JobMsg(
            s"ossPyJob${UUID.randomUUID().toString}",
            "job",
            "com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer.BPSPythonJobContainer",
            List("$BPSparkSession"),
            Nil,
            Nil,
            jobConfig,
            "",
            "temp job")
        val jobMsg = write(job)
        val topic = "stream_job_submit"
        val pkp = new PharbersKafkaProducer[String, BPJob]
        val bpJob = new BPJob(parentJobId, traceId, `type`, jobMsg)
        val fu = pkp.produce(topic, parentJobId, bpJob)
        println(fu.get(10, TimeUnit.SECONDS))
    }
}

object update extends App {
    val file = new Scanner(new File("D:\\文件\\weixin\\WeChat Files\\dengcao1993\\FileStorage\\File\\2019-12\\update(1)"))
    val mongoClient = MongoClients.create("mongodb://123.56.179.133:5555")
    val database = mongoClient.getDatabase("pharbers-sandbox-600")
    val assets = database.getCollection("assets", classOf[BsonDocument])
    while (file.hasNextLine){
        val name = file.nextLine()
//        val doc = assets.find(Filters.eq("description", name)).first()
//        doc.put("dfs", new BsonArray())
        assets.updateMany(Filters.eq("description", name), Document.parse("""{"$set":{"dfs":[]}}"""))
    }
}

class pm25 {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val df = spark.read.format("com.databricks.spark.csv")
            .option("header", true)
            .option("delimiter", ",")
            .load("file:///C:\\Users\\EDZ\\Desktop\\beijing_20190101-20191214\\beijing_20190101-20191214")
    df.selectExpr("date", "hour", "type", "cast(`东四环` as double) as value")
            .filter("value is not null and hour > 7 and hour < 21").groupBy("date", "type").avg("value")
            .filter("type = 'PM2.5' and avg(value) > 115")
            .show(365, false)
}


