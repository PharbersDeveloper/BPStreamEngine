package com.pharbers.StreamEngine.Jobs.EsJob.strategy

import com.pharbers.util.log.PhLogable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

case class BPSMaxDataHive2EsStrategy() extends BPSStrategy[DataFrame] with PhLogable {
    override def convert(data: DataFrame): DataFrame = {

        val keys: List[String] = "COMPANY" :: "SOURCE" :: "PROVINCE_NAME" :: "CITY_NAME" :: "PREFECTURE_NAME" :: "QUARTER" :: "HOSP_NAME" :: "HOSP_CODE" :: "HOSP_LEVEL" ::
            "ATC" :: "MOLE_NAME" :: "KEY_BRAND" :: "PRODUCT_NAME" :: "PACK" :: "SPEC" :: "DOSAGE" :: "PACK_QTY" :: "SALES_QTY" :: "SALES_VALUE" :: "DELIVERY_WAY" ::
            "MANUFACTURER_NAME" :: "MKT" :: "version" :: "YEAR" :: "MONTH" :: Nil
        //Check that the keys used in the aggregation are in the columns
        keys.foreach(k => {
            if (!data.columns.contains(k)) {
                logger.error(s"The key(${k}) used in the aggregation is not in the columns(${data.columns}).")
                return data
            }
        })
        
        val formatDF = data.selectExpr(keys.mkString(", "))
            .withColumn("YEAR", col("YEAR").cast(DataTypes.IntegerType))
            .withColumn("MONTH", col("MONTH").cast(DataTypes.IntegerType))
            .withColumn("YM", col("YEAR").*(100).+(col("MONTH")))

        return data
    }
}
