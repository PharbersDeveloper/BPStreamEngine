package com.pharbers.StreamEngine.BatchJobs.GenCubeJob.Strategies

import com.pharbers.StreamEngine.Utils.Log.PhLogable
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

case class WriteToEsStrategy() extends PhLogable {


    def writeListDF(listDF: List[DataFrame], index: String, mode: String = "append"): Unit = {

        logger.info("Start exec write list df to es strategy.")

        for (df <- listDF) {
            df.na.fill(0.0).write
                    .format("es")
                    .mode(mode)
                    .save(index)
        }

    }

    def writeDF(df: DataFrame, index: String, mode: String = "append"): Unit = {

        logger.info("Start exec write df to es strategy.")

        df.na.fill(0.0).write
            .format("es")
            .mode(mode)
            .save(index)

    }

}
