package com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer

import org.apache.spark.sql.SparkSession
import com.pharbers.StreamEngine.Jobs.PyJob.BPSPythonJob
import com.pharbers.StreamEngine.Utils.Schema.Spark.BPSParseSchema
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy

object BPSPythonJobContainer {
    def apply(strategy: BPSKfkJobStrategy, spark: SparkSession): BPSPythonJobContainer =
        new BPSPythonJobContainer(strategy, spark)
}

/** 执行 Python 的 Job
 *
 * @author clock
 * @version 0.1
 * @since 2019/11/6 17:43
 * @node 可用的配置参数
 * {{{
 *      hdfsAddr = "hdfs://spark.master:9000"
 *      resultPath = "hdfs:///test/sub/"
 *      metadata = Map("jobId" -> "a", "fileName" -> "b")
 * }}}
 */
class BPSPythonJobContainer(override val strategy: BPSKfkJobStrategy,
                            override val spark: SparkSession) extends BPSJobContainer with Serializable {

    val id = "57fe0-2bda-4880-8301-dc55a0" //UUID.randomUUID().toString
    type T = BPSKfkJobStrategy

    var metadata: Map[String, Any] = Map.empty

    val matedataPath = "hdfs:///test/alex/07b8411a-5064-4271-bfd3-73079f2b42b2/metadata/"
    val filesPath = "hdfs:///test/alex/07b8411a-5064-4271-bfd3-73079f2b42b2/files/"
    val resultPath = "hdfs:///test/qi/"

    override def open(): Unit = {
        metadata = BPSParseSchema.parseMetadata(matedataPath + id)(spark)
        val loadSchema = BPSParseSchema.parseSchema(metadata("schema").asInstanceOf[List[_]])

        val reading = spark.readStream
                .schema(loadSchema)
                .option("startingOffsets", "earliest")
                .parquet(filesPath + id)

        inputStream = Some(reading)
    }

    override def exec(): Unit = inputStream match {
        case Some(_) =>
            val job = BPSPythonJob(id, spark, inputStream, this, Map(
                "resultPath" -> resultPath,
                "metadata" -> metadata
            ))
            spark.sparkContext.addFile("./pyClean/main.py")
            spark.sparkContext.addFile("./pyClean/results.py")
            spark.sparkContext.addFile("./pyClean/auth.py")
            spark.sparkContext.addFile("./pyClean/mapping.py")
            spark.sparkContext.addFile("./pyClean/cleaning.py")
            job.open()
            job.exec()
        case None => ???
    }
}
