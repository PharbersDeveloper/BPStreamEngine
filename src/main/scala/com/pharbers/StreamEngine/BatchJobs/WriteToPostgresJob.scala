package com.pharbers.StreamEngine.BatchJobs

import com.pharbers.StreamEngine.Utils.Log.PhLogable
import org.apache.spark.sql.SparkSession



class WriteToPostgresJob(jobId: String, uri: String, dbUser: String, dbPass: String, dbTable: String, sparkSession: SparkSession) extends BPBatchJob with PhLogable {


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
        if (dbUser.isEmpty) {
            logger.error("No dbUser set!")
            sys.exit()
        }
        if (dbPass.isEmpty) {
            logger.error("No dbPass set!")
            sys.exit()
        }
        if (dbTable.isEmpty) {
            logger.error("No dbTable set!")
            sys.exit()
        }

        logger.info("WriteToPostgres start.")
        logger.info(s"WriteToPostgres uri=($uri) dbTable=($dbTable).")

        val reading = sparkSession.read.load(getJobStoragePath)

        reading.write
            .format("jdbc")
            .option("driver", "org.postgresql.Driver")
            .option("url", uri)
            .option("dbtable", dbTable)
            .option("user", dbUser)
            .option("password", dbPass)
            .mode("overwrite")
            .save()

        logger.info("WriteToPostgres done.")

    }

}
