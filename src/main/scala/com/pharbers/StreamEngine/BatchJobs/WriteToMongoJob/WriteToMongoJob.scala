package com.pharbers.StreamEngine.BatchJobs.WriteToMongoJob

import com.pharbers.StreamEngine.BatchJobs.BPBatchJob
import com.pharbers.StreamEngine.BatchJobs.CommonStrategy.WriteStrategy
import com.pharbers.StreamEngine.BatchJobs.CommonStrategy.WriteStrategy._
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Log.PhLogable
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.spark.sql.SparkSession

object WriteToMongoJob {

    def apply(jobId: String, uri: String, dbName: String, collName: String): WriteToMongoJob = {
        val sparkSession: SparkSession = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession].spark
        new WriteToMongoJob(jobId, uri, dbName, collName, sparkSession)
    }

}

class WriteToMongoJob(jobId: String, uri: String, dbName: String, collName: String, sparkSession: SparkSession) extends BPBatchJob with PhLogable {


    override val id: String = jobId

    def start = {

        if (jobId.isEmpty) {
            logger.error("No jobId set!")
            sys.exit()
        }
        if (uri.isEmpty) {
            logger.error("No uri set!")
            sys.exit()
        }
        if (dbName.isEmpty) {
            logger.error("No dbName set!")
            sys.exit()
        }
        if (collName.isEmpty) {
            logger.error("No collName set!")
            sys.exit()
        }

        logger.info("GenCubeJob start.")
        logger.info(s"GenCubeJob uri=($uri) dbName=($dbName) collName=($collName).")

        val reading = sparkSession.read.load(getJobStoragePath)

        val options: Map[String, String] = Map{
            "uri" -> uri
            "database" -> dbName
            "collection" -> collName
        }

        WriteStrategy(MONGO_SOURCE_TYPE).writeDF(df = reading, path = collName, options = options)

        logger.info("GenCubeJob done.")

    }

}
