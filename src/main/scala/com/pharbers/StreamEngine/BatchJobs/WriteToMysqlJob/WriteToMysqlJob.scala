package com.pharbers.StreamEngine.BatchJobs.WriteToMysqlJob

import java.util.Properties

import com.pharbers.StreamEngine.BatchJobs.BPBatchJob
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Log.PhLogable
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.spark.sql.{SaveMode, SparkSession}

object WriteToMysqlJob {

    def apply(jobId: String, uri: String, dbName: String, collName: String, username: String, password: String): WriteToMysqlJob = {
        val sparkSession: SparkSession = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession].spark
        new WriteToMysqlJob(jobId, uri, dbName, collName, username, password, sparkSession)
    }

}

class WriteToMysqlJob(jobId: String, uri: String, dbName: String, collName: String, username: String, password: String, sparkSession: SparkSession) extends BPBatchJob with PhLogable {


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
        if (username.isEmpty) {
            logger.error("No username set!")
            sys.exit()
        }
        if (password.isEmpty) {
            logger.error("No password set!")
            sys.exit()
        }

        logger.info("WriteToMysqlJob start.")
        logger.info(s"WriteToMysqlJob uri=($uri) dbName=($dbName) collName=($collName).")

        val reading = sparkSession.read.load(getJobStoragePath)

        val prop = new Properties()
        prop.setProperty("user", username)
        prop.setProperty("password", password)

        reading.na.fill(0.0).write.mode(SaveMode.Overwrite).jdbc(s"jdbc:mysql://${uri}/${dbName}", collName, prop)

        logger.info("WriteToMysqlJob done.")

    }

}
