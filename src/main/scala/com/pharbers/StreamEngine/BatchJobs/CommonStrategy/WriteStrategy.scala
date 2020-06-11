package com.pharbers.StreamEngine.BatchJobs.CommonStrategy

import com.pharbers.StreamEngine.Utils.Log.PhLogable
import org.apache.spark.sql.DataFrame

object WriteStrategy {

    final val ES_SOURCE_TYPE: String = "es"
    final val PARQUET_SOURCE_TYPE: String = "parquet"
    final val CSV_SOURCE_TYPE: String = "csv"
    final val MONGO_SOURCE_TYPE: String = "com.mongodb.spark.sql.DefaultSource"

    def apply(sourceType: String): WriteStrategy = new WriteStrategy(sourceType)
}

//sourceType -> es / parquet / csv / etc...
class WriteStrategy(sourceType: String) extends PhLogable {


    def writeListDF(listDF: List[DataFrame], path: String, mode: String = "append", options: Map[String, String] = Map.empty): Unit = {

        logger.info(s"Start exec write list df to sourceType($sourceType) path($path) strategy.")

        for (df <- listDF) {
            df.na.fill(0.0).write
                    .format(sourceType)
                    .options(options)
                    .mode(mode)
                    .save(path)
        }

    }

    def writeDF(df: DataFrame, path: String, mode: String = "append", options: Map[String, String] = Map.empty): Unit = {

        logger.info(s"Start exec write df to sourceType($sourceType) path($path) strategy.")

        df.na.fill(0.0).write
            .format(sourceType)
            .options(options)
            .mode(mode)
            .save(path)

    }

}
