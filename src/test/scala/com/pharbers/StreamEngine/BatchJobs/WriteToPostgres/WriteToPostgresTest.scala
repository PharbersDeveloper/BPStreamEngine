package com.pharbers.StreamEngine.BatchJobs.WriteToPostgres

import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Log.PhLogable
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class WriteToPostgresTest extends FunSuite with PhLogable  {

    test("WriteToPostgres Test") {

        val sparkSession: SparkSession = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession].spark

//        val reading = sparkSession.sql("SELECT * FROM max_result LIMIT 10")

        val user = sys.env.getOrElse("POSTGRES_USER", "not found")
        if (user == "not found") {
            logger.error("No POSTGRES_USER set!")
            sys.exit()
        }
        val pass = sys.env.getOrElse("POSTGRES_PASS", "not found")
        if (pass == "not found") {
            logger.error("No POSTGRES_PASS set!")
            sys.exit()
        }

        val uri = "jdbc:postgresql://ph-db-lambda.cngk1jeurmnv.rds.cn-northwest-1.amazonaws.com.cn/phentry"
        val dbTable = "testFromSpark"

        import sparkSession.implicits._
        val df = Seq(
            ("first", 2.0),
            ("test", 1.5),
            ("choose", 8.0)
        ).toDF("id", "val")

        df.write
            .format("jdbc")
            .option("url", uri)
            .option("dbtable", dbTable)
            .option("user", user)
            .option("password", pass)
            .mode("overwrite")
            .save()

       }

}
