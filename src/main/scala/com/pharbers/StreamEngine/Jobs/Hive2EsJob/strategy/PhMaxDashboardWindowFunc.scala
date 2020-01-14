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

    def GenerateSalesRowWith(time: String, prodLevel: String, region: String): PhMaxDashboardWindowFunc = {

        val prodLevelKeys: List[String] = prodLevel match {
            case "MOLE" => "PRODUCT_NAME" :: "MOLE_NAME" :: Nil
            case "PROD" => "PRODUCT_NAME" :: Nil
            case _ => logger.error(s"${prodLevel} not implemented."); Nil
        }

        val regionKeys: List[String] = region match {
            case "CITY" => "PROVINCE" :: "CITY" :: Nil
            case "PROV" => "PROVINCE" :: Nil
            case "NATION" => Nil
            case _ => logger.error(s"${prodLevel} not implemented."); Nil
        }

        val mktWindow: WindowSpec = Window.partitionBy("MKT", prodLevelKeys ::: regionKeys: _*)

        val newDF: DataFrame = time match {
            case "CURR" => df
                .withColumn(time + "_" + prodLevel + "_SALES_IN_" + region, sum("MOLE_SALES_VALUE").over(currMonthWindow(mktWindow)))
            case "LAST_M" => df
                .withColumn(time + "_" + prodLevel + "_SALES_IN_" + region, sum("MOLE_SALES_VALUE").over(lastMonthWindow(mktWindow)))
            case "LAST_Y" => df
                .withColumn(time + "_" + prodLevel + "_SALES_IN_" + region, sum("MOLE_SALES_VALUE").over(lastYearWindow(mktWindow)))
            case _ => logger.error(s"${time} not implemented."); df
        }

        PhMaxDashboardWindowFunc(newDF)
    }

    def GenerateRankAndShareRowWith(time: String, prodLevel: String, childRegion: String, fatherRegion: String): PhMaxDashboardWindowFunc = {
        val prodLevelRankKeys: List[String] = prodLevel match {
            case "MOLE" => "PRODUCT_NAME" :: Nil
            case "PROD" => Nil
            case _ => logger.error(s"${prodLevel} not implemented."); Nil
        }

        val regionRankKeys: List[String] = fatherRegion match {
            case "CITY" => "PROVINCE" :: "CITY" :: Nil
            case "PROV" => "PROVINCE" :: Nil
            case "NATION" => Nil
            case _ => logger.error(s"${prodLevel} not implemented."); Nil
        }

        val rankWindow: WindowSpec = Window.partitionBy("DATE", "MKT" :: prodLevelRankKeys ::: regionRankKeys: _*)

        val newDF: DataFrame = df
            .withColumn(time + "_" + prodLevel + "_" + childRegion + "_RANK_IN_" + fatherRegion, dense_rank.over(rankWindow.orderBy(col(time + "_" + prodLevel + "_SALES_IN_" + childRegion).desc)))
            .withColumn(time + "_" + prodLevel + "_" + childRegion + "_SHARE_IN_" + fatherRegion, col(time + "_" + prodLevel + "_SALES_IN_" + childRegion) / col(time + "_" + prodLevel + "_SALES_IN_" + fatherRegion))

        PhMaxDashboardWindowFunc(newDF)
    }

    def GenerateGrowthRowWith(time: String, prodLevel: String, region: String): PhMaxDashboardWindowFunc = {
        val newDF: DataFrame = time match {
            case "MOM" => df
                .withColumn("MOM_GROWTH_ON_" + prodLevel + "_" + region + "LEVEL", when(col("LAST_M_" + prodLevel + "_SALES_IN_" + region) === 0.0, 0.0).otherwise(col("CURR_" + prodLevel + "_SALES_IN_" + region) - col("LAST_M_" + prodLevel + "_SALES_IN_" + region)))
                .withColumn("MOM_RATE_ON_" + prodLevel + "_" + region + "LEVEL", when(col("LAST_M_" + prodLevel + "_SALES_IN_" + region) === 0.0, 0.0).otherwise((col("CURR_" + prodLevel + "_SALES_IN_" + region) - col("LAST_M_" + prodLevel + "_SALES_IN_" + region)) / col("LAST_M_" + prodLevel + "_SALES_IN_" + region)))
            case "YOY" => df
                .withColumn("YOY_GROWTH_ON_" + prodLevel + "_" + region + "LEVEL", when(col("LAST_Y_" + prodLevel + "_SALES_IN_" + region) === 0.0, 0.0).otherwise(col("CURR_" + prodLevel + "_SALES_IN_" + region) - col("LAST_Y_" + prodLevel + "_SALES_IN_" + region)))
                .withColumn("YOY_RATE_ON_" + prodLevel + "_" + region + "LEVEL", when(col("LAST_Y_" + prodLevel + "_SALES_IN_" + region) === 0.0, 0.0).otherwise((col("CURR_" + prodLevel + "_SALES_IN_" + region) - col("LAST_Y_" + prodLevel + "_SALES_IN_" + region)) / col("LAST_M_" + prodLevel + "_SALES_IN_" + region)))
            case _ => logger.error(s"${time} not implemented."); df
        }

        PhMaxDashboardWindowFunc(newDF)
    }

    def GenerateEIRowWith(prodLevel: String, childRegion: String, fatherRegion: String): PhMaxDashboardWindowFunc = {

        val newDF: DataFrame = df
            .withColumn("EI_" + prodLevel + "_" + childRegion + "_OVER_" + fatherRegion, when(col("LAST_M_" + prodLevel + "_" + childRegion + "_SHARE_IN_" + fatherRegion) === 0.0, 0.0).otherwise(col("LAST_M_" + prodLevel + "_" + childRegion + "_SHARE_IN_" + fatherRegion) / col("CURR_" + prodLevel + "_" + childRegion + "_SHARE_IN_" + fatherRegion)))

        PhMaxDashboardWindowFunc(newDF)
    }
    
}
