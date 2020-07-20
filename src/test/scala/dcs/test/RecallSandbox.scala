package dcs.test

import java.io.File
import java.util.{Scanner, UUID}
import java.util.concurrent.TimeUnit

import com.mongodb.client.MongoClients
import com.mongodb.client.model.Filters
import com.pharbers.StreamEngine.Utils.Component.Dynamic.JobMsg
import com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.PharbersKafkaProducer
import com.pharbers.kafka.schema.BPJob
import org.apache.spark.SparkConf
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
    val spark = SparkSession.builder().config(new SparkConf().setMaster("local[2]")).enableHiveSupport().getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", sys.env("AWS_ACCESS_KEY_ID"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", sys.env("AWS_SECRET_ACCESS_KEY_ID"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")
    spark.sparkContext.setLogLevel("WARN")
    val file = new Scanner(new File("C:\\Users\\EDZ\\Desktop\\clean_job_id"))
    val mongoClient = MongoClients.create("mongodb://pharbers:Pharbers.84244216@a0eb74da798af11ea934b02ff50af4f4-919388620.cn-northwest-1.elb.amazonaws.com.cn:30010")
    val database = mongoClient.getDatabase("pharbers-sandbox-merge")
    val jobs = database.getCollection("jobs", classOf[BsonDocument])
    val datasets = database.getCollection("datasets", classOf[BsonDocument])
    val assets = database.getCollection("assets", classOf[BsonDocument])
    val files = database.getCollection("files", classOf[BsonDocument])
    while (file.hasNextLine){
        val jobId = file.nextLine()
        val doc = jobs.find(Filters.eq("jobContainerId", jobId)).first()
        if(doc != null) {
            val jobMongoId = doc.getObjectId("_id")
            val dataset = datasets.find(Filters.eq("job", jobMongoId)).first()
            val path = dataset.getString("url").getValue
            val datasetId = dataset.getObjectId("_id")
            val asset = assets.find(Filters.in("dfs", datasetId)).first()
            val fileId = asset.getObjectId("file")
            val url = files.find(Filters.eq("_id", fileId)).first().getString("url").getValue
            val sheet = files.find(Filters.eq("_id", fileId)).first().getString("sheetName").getValue
            val label = files.find(Filters.eq("_id", fileId)).first().getString("label").getValue
            val err = spark.sparkContext.textFile(path.replace("contents", "err")).take(1).head
            println(s"$url**$sheet**$jobId**$err")
//            if(sheet == "others"){
//                files.updateOne(Filters.eq("_id", fileId),Document.parse("""{"$set":{"label":"其他"}}"""))
//            }
//            if(sheet.contains("未到医院名单及说明")){
//                files.updateOne(Filters.eq("_id", fileId),Document.parse("""{"$set":{"label":"未到名单"}}"""))
//            }
//            if(sheet.contains("附表")){
//                files.updateOne(Filters.eq("_id", fileId),Document.parse("""{"$set":{"label":"附表"}}"""))
//            }

        }
    }
}

object deleteDfs extends App{
    val mongoClient = MongoClients.create("mongodb://pharbers:Pharbers.84244216@a0eb74da798af11ea934b02ff50af4f4-919388620.cn-northwest-1.elb.amazonaws.com.cn:30010")
    val database = mongoClient.getDatabase("pharbers-sandbox-merge")
    val jobs = database.getCollection("jobs", classOf[BsonDocument])
    val datasets = database.getCollection("datasets", classOf[BsonDocument])
    val assets = database.getCollection("assets", classOf[BsonDocument])
    val files = database.getCollection("files", classOf[BsonDocument])
    val dataset = datasets.find(Filters.eq("description", "pyJob")).iterator()
    while (dataset.hasNext){
        val doc = dataset.next()
        val asset = assets.find(Filters.in("dfs", doc.getObjectId("_id").getValue)).first()
        if(asset == null){
            datasets.deleteOne(Filters.eq("_id", doc.getObjectId("_id").getValue))
        }
    }
}

object deleteCpaDfs extends App{
    val mongoClient = MongoClients.create("mongodb://pharbers:Pharbers.84244216@a0eb74da798af11ea934b02ff50af4f4-919388620.cn-northwest-1.elb.amazonaws.com.cn:30010")
    val database = mongoClient.getDatabase("pharbers-sandbox-merge")
    val jobs = database.getCollection("jobs", classOf[BsonDocument])
    val datasets = database.getCollection("datasets", classOf[BsonDocument])
    val assets = database.getCollection("assets", classOf[BsonDocument])
    val files = database.getCollection("files", classOf[BsonDocument])
    val iterator = assets.find(Document.parse("""{"providers":{$size:2}}""")).iterator()
    val cpas = List("CPA", "CPA&GYC", "CPA&PTI&DDD&HH", "GYC&CPA", "GYC")
    var count = 0
    while (iterator.hasNext){
        val asset = iterator.next()
        val providers = asset.getArray("providers").getValues.get(1).asString().getValue
        val isNew = asset.getBoolean("isNewVersion").getValue
        val dfs = asset.getArray("dfs").getValues
        if(cpas.contains(providers) && isNew && dfs.size() == 2){

//            val fileId = asset.getObjectId("file")
//            val file = files.find(Filters.eq("_id", fileId)).first()
//            println(file.getString("label").getValue)
//            val dfs = asset.getArray("dfs").getValues.get(1).asObjectId().getValue
//            val dataset = datasets.find(Filters.eq("_id", dfs)).first()
//            val url = dataset.getString("url").getValue
//            assets.updateOne(Filters.eq("_id", asset.getObjectId("_id").getValue),Document.parse("""{"$set":{"dfs":[]}}"""))
            count += 1
        }
    }
    println(count)
}

object finLabel extends App{
    val mongoClient = MongoClients.create("mongodb://pharbers:Pharbers.84244216@a0eb74da798af11ea934b02ff50af4f4-919388620.cn-northwest-1.elb.amazonaws.com.cn:30010")
    val database = mongoClient.getDatabase("pharbers-sandbox-merge")
    val jobs = database.getCollection("jobs", classOf[BsonDocument])
    val datasets = database.getCollection("datasets", classOf[BsonDocument])
    val assets = database.getCollection("assets", classOf[BsonDocument])
    val files = database.getCollection("files", classOf[BsonDocument])

    val cpas = List("CPA", "CPA&GYC", "CPA&PTI&DDD&HH", "GYC&CPA", "GYC")
    var providers: List[String] = Nil
    val iterator = files.find(Document.parse("""{"label":"原始数据"}""")).iterator()
    while (iterator.hasNext){
        val file = iterator.next()
        val fileId = file.getObjectId("_id").getValue
        val asset = assets.find(Filters.eq("file", fileId)).first()
        val provider = asset.getArray("providers").getValues.get(1).asString().getValue
        providers = providers :+ provider
    }
    providers.distinct.foreach(println)
}

object resetDfs extends App{
    val jobIds = "clean_job_5f0f2a84c754120001e60967\nclean_job_5f0f5257c754120001e609ef\nclean_job_5f0f54b0c754120001e609f7\nclean_job_5f0f5a8fc754120001e60a0b\nclean_job_5f0f8979c754120001e60aab\nclean_job_5f0fb01fc754120001e60b2f\nclean_job_5f0fb143c754120001e60b33\nclean_job_5f0fb399c754120001e60b3b\nclean_job_5f0fbaa6c754120001e60b53\nclean_job_5f0fbbccc754120001e60b57\nclean_job_5f0fbcfdc754120001e60b5b\nclean_job_5f0fbe2ac754120001e60b5f\nclean_job_5f0fbf55c754120001e60b63"
    val mongoClient = MongoClients.create("mongodb://pharbers:Pharbers.84244216@a0eb74da798af11ea934b02ff50af4f4-919388620.cn-northwest-1.elb.amazonaws.com.cn:30010")
    val database = mongoClient.getDatabase("pharbers-sandbox-merge")
    val jobs = database.getCollection("jobs", classOf[BsonDocument])
    val datasets = database.getCollection("datasets", classOf[BsonDocument])
    val assets = database.getCollection("assets", classOf[BsonDocument])
    val files = database.getCollection("files", classOf[BsonDocument])
    val dataset = datasets.find(Filters.eq("description", "pyJob")).iterator()
    jobIds.split("\n").foreach(jobId => {
        val doc = jobs.find(Filters.eq("jobContainerId", jobId)).first()
        val jobMongoId = doc.getObjectId("_id")
        val dataset = datasets.find(Filters.eq("job", jobMongoId)).first()
        val datasetId = dataset.getObjectId("_id")
        val asset = assets.find(Filters.in("dfs", datasetId)).first()
        val assetId = asset.getObjectId("_id")
        assets.updateOne(Filters.eq("_id", assetId),Document.parse("""{"$set":{"dfs":[]}}"""))
    })
}


