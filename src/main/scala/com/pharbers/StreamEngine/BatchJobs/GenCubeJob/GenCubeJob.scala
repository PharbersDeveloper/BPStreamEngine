package com.pharbers.StreamEngine.BatchJobs.GenCubeJob

import java.util.UUID

import com.pharbers.StreamEngine.BatchJobs.BPBatchJob
import com.pharbers.StreamEngine.BatchJobs.GenCubeJob.Strategies.{DataCleanStrategy, GenCubeJobStrategy, WriteToEsStrategy}
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Log.PhLogable
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.spark.sql.SparkSession

object GenCubeJob {

    def apply(): GenCubeJob = {
        val jobId: String = UUID.randomUUID().toString
        val sparkSession: SparkSession = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession].spark
        new GenCubeJob(jobId, sparkSession)
    }

}

class GenCubeJob(jobId: String, sparkSession: SparkSession) extends BPBatchJob with PhLogable {

    def start = {

        logger.info("GenCubeJob start.")
        val config = JobConfig()
        val sql = config("sql")
        logger.info(s"GenCubeJob sql=($sql).")
        val esIndex = config("esIndex")
        logger.info(s"GenCubeJob esIndex=($esIndex).")

        val reading = sparkSession.sql(sql)
        WriteToEsStrategy().writeDF(reading, esIndex)
//        logger.info("GenCubeJob origin length =  ========>" + reading.count())
//
//        val cleanDF = new DataCleanStrategy(sparkSession).clean(reading)
//        logger.info("GenCubeJob cleanDF length =  ========>" + cleanDF.count())
//
//        val cubeDF = new GenCubeJobStrategy(sparkSession).convert(cleanDF)
//        WriteToEsStrategy().writeListDF(cubeDF, esIndex)

        logger.info("GenCubeJob done.")
        sys.exit()

    }

    private def JobConfig(): Map[String, String] = {
        val config: Map[String, String] = Map.empty
        val sql = sys.env.getOrElse("GEN_CUBE__SQL", {
            logger.error("No env GEN_CUBE__SQL found!")
            sys.exit()
        })
        val esIndex = sys.env.getOrElse("GEN_CUBE__ES_INDEX", {
            logger.error("No env GEN_CUBE__ES_INDEX found!")
            sys.exit()
        })
        config + ("sql" -> sql) + ("esIndex" -> esIndex)
    }

}
