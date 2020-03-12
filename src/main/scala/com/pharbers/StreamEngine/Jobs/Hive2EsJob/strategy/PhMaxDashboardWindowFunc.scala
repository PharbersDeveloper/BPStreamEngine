package com.pharbers.StreamEngine.Jobs.Hive2EsJob.strategy

import com.pharbers.util.log.PhLogable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

case class PhMaxDashboardWindowFunc(df: DataFrame) extends PhLogable {

    private def currMonthWindow(window: WindowSpec): WindowSpec = {
        window.orderBy(col("DATE").cast(DataTypes.IntegerType)).rangeBetween(0, 0)
    }

    private def lastMonthWindow(window: WindowSpec): WindowSpec = {
        window
            .orderBy(to_date(col("DATE").cast("string"), "yyyyMM").cast("timestamp").cast("long"))
            .rangeBetween(-86400 * 31, -86400 * 28)
    }

    private def lastYearWindow(window: WindowSpec): WindowSpec =
        window.orderBy(col("DATE").cast(DataTypes.IntegerType)).rangeBetween(-100, -100)

    def GenerateSalesAndRankRowWith(time: String, prodLevel: String, region: String): PhMaxDashboardWindowFunc = {

        val prodSalesKeys: List[String] = prodLevel match {
            case "MOLE" => "PRODUCT_NAME" :: "MOLE_NAME" :: Nil
            case "PROD" => "PRODUCT_NAME" :: Nil
            case "MKT" => "MKT" :: Nil
            case "TOTAL" => Nil
            case _ => logger.error(s"${prodLevel} not implemented."); Nil
        }

        val prodRankKeys: List[String] = prodLevel match {
            case "MOLE" => "PRODUCT_NAME" :: Nil
            case "PROD" => Nil
            case "MKT" => Nil
            case "TOTAL" => Nil
            case _ => logger.error(s"${prodLevel} not implemented."); Nil
        }

        val regionKeys: List[String] = region match {
            case "CITY" => "PROVINCE" :: "CITY" :: Nil
            case "PROV" => "PROVINCE" :: Nil
            case "NATION" => Nil
            case _ => logger.error(s"${region} not implemented."); Nil
        }

        val mktWindow: WindowSpec = Window.partitionBy("COMPANY", prodSalesKeys ::: regionKeys: _*)
        val rankWindow: WindowSpec = Window.partitionBy("DATE", prodRankKeys ::: regionKeys: _*)

        val newDF: DataFrame = time match {
            case "CURR" => df
                .withColumn(time + "_" + prodLevel + "_SALES_IN_" + region, sum("MOLE_SALES_VALUE").over(currMonthWindow(mktWindow)))
                .withColumn(time + "_" + prodLevel + "_RANK_IN_" + region, dense_rank.over(rankWindow.orderBy(col(time + "_" + prodLevel + "_SALES_IN_" + region).desc)))
            case "LAST_M" => df
                .withColumn(time + "_" + prodLevel + "_SALES_IN_" + region, sum("MOLE_SALES_VALUE").over(lastMonthWindow(mktWindow)))
                .withColumn(time + "_" + prodLevel + "_RANK_IN_" + region, dense_rank.over(rankWindow.orderBy(col(time + "_" + prodLevel + "_SALES_IN_" + region).desc)))
            case "LAST_Y" => df
                .withColumn(time + "_" + prodLevel + "_SALES_IN_" + region, sum("MOLE_SALES_VALUE").over(lastYearWindow(mktWindow)))
                .withColumn(time + "_" + prodLevel + "_RANK_IN_" + region, dense_rank.over(rankWindow.orderBy(col(time + "_" + prodLevel + "_SALES_IN_" + region).desc)))
            case _ => logger.error(s"${time} not implemented."); df
        }

        PhMaxDashboardWindowFunc(newDF)
    }

    def GenerateShareRowWith(time: String, prodLevel: String, region: String): PhMaxDashboardWindowFunc = {

        val newDF: DataFrame = df
            .withColumn(time + "_" + prodLevel + "_" + region + "_SHARE", when(col(time + "_TOTAL_SALES_IN_" + region) === 0.0, 0.0).otherwise(col(time + "_" + prodLevel + "_SALES_IN_" + region) / col(time + "_TOTAL_SALES_IN_" + region)))

        PhMaxDashboardWindowFunc(newDF)
    }

    def GenerateGrowthRowWith(time: String, prodLevel: String, region: String): PhMaxDashboardWindowFunc = {
        val newDF: DataFrame = time match {
            case "MOM" => df
                .withColumn("MOM_GROWTH_ON_" + prodLevel + "_" + region, when(col("LAST_M_" + prodLevel + "_SALES_IN_" + region) === 0.0, 0.0).otherwise(col("CURR_" + prodLevel + "_SALES_IN_" + region) - col("LAST_M_" + prodLevel + "_SALES_IN_" + region)))
                .withColumn("MOM_RATE_ON_" + prodLevel + "_" + region, when(col("LAST_M_" + prodLevel + "_SALES_IN_" + region) === 0.0, 0.0).otherwise((col("CURR_" + prodLevel + "_SALES_IN_" + region) - col("LAST_M_" + prodLevel + "_SALES_IN_" + region)) / col("LAST_M_" + prodLevel + "_SALES_IN_" + region)))
            case "YOY" => df
                .withColumn("YOY_GROWTH_ON_" + prodLevel + "_" + region, when(col("LAST_Y_" + prodLevel + "_SALES_IN_" + region) === 0.0, 0.0).otherwise(col("CURR_" + prodLevel + "_SALES_IN_" + region) - col("LAST_Y_" + prodLevel + "_SALES_IN_" + region)))
                .withColumn("YOY_RATE_ON_" + prodLevel + "_" + region, when(col("LAST_Y_" + prodLevel + "_SALES_IN_" + region) === 0.0, 0.0).otherwise((col("CURR_" + prodLevel + "_SALES_IN_" + region) - col("LAST_Y_" + prodLevel + "_SALES_IN_" + region)) / col("LAST_M_" + prodLevel + "_SALES_IN_" + region)))
            case _ => logger.error(s"${time} not implemented."); df
        }

        PhMaxDashboardWindowFunc(newDF)
    }

    def GenerateEIRowWith(prodLevel: String, region: String): PhMaxDashboardWindowFunc = {

        val newDF: DataFrame = df
            .withColumn("EI_" + prodLevel + "_" + region, when(col("LAST_M_" + prodLevel + "_" + region + "_SHARE") === 0.0, 0.0).otherwise(col("CURR_" + prodLevel + "_" + region + "_SHARE") / col("LAST_M_" + prodLevel + "_" + region + "_SHARE")))

        PhMaxDashboardWindowFunc(newDF)
    }
    
}
