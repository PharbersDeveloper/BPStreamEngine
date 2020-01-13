package com.pharbers.StreamEngine.Jobs.Hive2EsJob.strategy

import com.pharbers.util.log.PhLogable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

case class BPSMaxDataHive2EsStrategy(spark: SparkSession) extends BPSStrategy[DataFrame] with PhLogable {

    override def convert(data: DataFrame): DataFrame = {
        //TODO:筛选出 max_dashboard 需要用到的 keys 即可
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

        val formatDF = data.selectExpr(keys: _*)
            .withColumn("YEAR", col("YEAR").cast(DataTypes.IntegerType))
            .withColumn("MONTH", col("MONTH").cast(DataTypes.IntegerType))
            .withColumn("YM", col("YEAR") * 100 + col("MONTH") )
            .withColumn("SALES_VALUE", col("SALES_VALUE").cast(DataTypes.DoubleType))

        //缩小数据范围，需求中最小维度是分子，先计算出分子级别在单个公司年月市场、省&城市级别、产品&分子维度的聚合数据
        val moleLevelDF = formatDF.groupBy("COMPANY", "YM", "MKT", "PROVINCE_NAME", "CITY_NAME", "PRODUCT_NAME", "MOLE_NAME")
            .agg(expr("SUM(SALES_VALUE) as MOLE_SALES_VALUE"))

        //TODO:因不同公司数据的数据时间维度不一样，所以分别要对每个公司的数据进行计算最新一年的数据
        val companyList = moleLevelDF.select("COMPANY").distinct().collect().map(_(0)).toList.asInstanceOf[List[String]]

        companyList.map(company => {
            val companyDF = moleLevelDF.filter(col("COMPANY") === company)
            //TODO:得保证数据源中包含两年的数据
            val current2YearYmList = companyDF.select("YM").distinct().sort("YM").collect().map(_(0)).toList.takeRight(24).asInstanceOf[List[Int]]
            val currentYearYmList = current2YearYmList.takeRight(12)

            val current2YearDF = companyDF
                .filter(col("YM") >= current2YearYmList.min.toString.toInt && col("YM") <= current2YearYmList.max.toString.toInt)
            val currentYearDF = computeMaxDashboardData(current2YearDF)
                .filter(col("YM") >= currentYearYmList.min.toString.toInt && col("YM") <= currentYearYmList.max.toString.toInt)
                .cache()
            currentYearDF
        }).reduce((x, y) => x union y)

    }

    def computeMaxDashboardData(df: DataFrame): DataFrame = {
        val moleWindow = Window
            .partitionBy("MKT", "PROVINCE_NAME", "CITY_NAME", "PRODUCT_NAME", "MOLE_NAME")

        val prodWindow = Window
            .partitionBy("MKT", "PROVINCE_NAME", "CITY_NAME", "PRODUCT_NAME")

        val cityWindow = Window
            .partitionBy("MKT", "PROVINCE_NAME", "CITY_NAME")

        val provWindow = Window
            .partitionBy("MKT", "PROVINCE_NAME")

        val mktWindow = Window
            .partitionBy("MKT")

        //TODO:产品在不同范围内的销售额和份额都是不同的，PROD_IN_CITY,PROD_IN_PROV,PROD_IN_MKT,PROD_IN_COMPANY
        //TODO:数据是否正确需要核对
        df.withColumn("PROD_SALES_VALUE", sum("MOLE_SALES_VALUE").over(currMonthWindow(prodWindow)))
            .withColumn("PROD_SALES_IN_COMPANY_VALUE", sum("MOLE_SALES_VALUE").over(currMonthWindow(Window.partitionBy("PRODUCT_NAME"))))
            .withColumn("PROD_SALES_IN_COMPANY_RANK", dense_rank.over(Window.partitionBy("YM").orderBy(col("PROD_SALES_IN_COMPANY_VALUE").desc)))
            .withColumn("CITY_SALES_VALUE", sum("MOLE_SALES_VALUE").over(currMonthWindow(cityWindow)))
            .withColumn("PROV_SALES_VALUE", sum("MOLE_SALES_VALUE").over(currMonthWindow(provWindow)))
            .withColumn("MKT_SALES_VALUE", sum("MOLE_SALES_VALUE").over(currMonthWindow(mktWindow)))
            .withColumn("MOLE_IN_PROD_SHARE", col("MOLE_SALES_VALUE") / col("PROD_SALES_VALUE"))
            .withColumn("MOLE_IN_CITY_SHARE", col("MOLE_SALES_VALUE") / col("CITY_SALES_VALUE"))
            .withColumn("MOLE_IN_PROV_SHARE", col("MOLE_SALES_VALUE") / col("PROV_SALES_VALUE"))
            .withColumn("MOLE_IN_MKT_SHARE", col("MOLE_SALES_VALUE") / col("MKT_SALES_VALUE"))
            .withColumn("PROD_IN_CITY_SHARE", col("PROD_SALES_VALUE") / col("CITY_SALES_VALUE"))
            .withColumn("PROD_IN_PROV_SHARE", col("PROD_SALES_VALUE") / col("PROV_SALES_VALUE"))
            .withColumn("PROD_IN_MKT_SHARE", col("PROD_SALES_VALUE") / col("MKT_SALES_VALUE"))
            .withColumn("CITY_IN_PROV_SHARE", col("CITY_SALES_VALUE") / col("PROV_SALES_VALUE"))
            .withColumn("CITY_IN_MKT_SHARE", col("CITY_SALES_VALUE") / col("MKT_SALES_VALUE"))
            .withColumn("PROV_IN_MKT_SHARE", col("PROV_SALES_VALUE") / col("MKT_SALES_VALUE"))
            //fill lastMonth values
            .withColumn("LAST_M_MOLE_SALES_VALUE", first("MOLE_SALES_VALUE").over(lastMonthWindow(prodWindow)))
            .na.fill(Map("LAST_M_MOLE_SALES_VALUE" -> 0.0))
            .withColumn("LAST_M_PROD_SALES_VALUE", first("PROD_SALES_VALUE").over(lastMonthWindow(prodWindow)))
            .na.fill(Map("LAST_M_PROD_SALES_VALUE" -> 0.0))
            .withColumn("LAST_M_CITY_SALES_VALUE", first("CITY_SALES_VALUE").over(lastMonthWindow(cityWindow)))
            .na.fill(Map("LAST_M_CITY_SALES_VALUE" -> 0.0))
            .withColumn("LAST_M_PROV_SALES_VALUE", first("PROV_SALES_VALUE").over(lastMonthWindow(provWindow)))
            .na.fill(Map("LAST_M_PROV_SALES_VALUE" -> 0.0))
            .withColumn("LAST_M_MKT_SALES_VALUE", first("MKT_SALES_VALUE").over(lastMonthWindow(mktWindow)))
            .na.fill(Map("LAST_M_MKT_SALES_VALUE" -> 0.0))
            .withColumn("LAST_M_MOLE_IN_PROD_SHARE", when(col("LAST_M_PROD_SALES_VALUE") === 0.0, 0.0).otherwise(col("LAST_M_MOLE_SALES_VALUE") / col("LAST_M_PROD_SALES_VALUE")))
            .withColumn("LAST_M_MOLE_IN_CITY_SHARE", when(col("LAST_M_CITY_SALES_VALUE") === 0.0, 0.0).otherwise(col("LAST_M_MOLE_SALES_VALUE") / col("LAST_M_CITY_SALES_VALUE")))
            .withColumn("LAST_M_MOLE_IN_PROV_SHARE", when(col("LAST_M_PROV_SALES_VALUE") === 0.0, 0.0).otherwise(col("LAST_M_MOLE_SALES_VALUE") / col("LAST_M_PROV_SALES_VALUE")))
            .withColumn("LAST_M_MOLE_IN_MKT_SHARE", when(col("LAST_M_MKT_SALES_VALUE") === 0.0, 0.0).otherwise(col("LAST_M_MOLE_SALES_VALUE") / col("LAST_M_MKT_SALES_VALUE")))
            .withColumn("LAST_M_PROD_IN_CITY_SHARE", when(col("LAST_M_CITY_SALES_VALUE") === 0.0, 0.0).otherwise(col("LAST_M_PROD_SALES_VALUE") / col("LAST_M_CITY_SALES_VALUE")))
            .withColumn("LAST_M_PROD_IN_PROV_SHARE", when(col("LAST_M_PROV_SALES_VALUE") === 0.0, 0.0).otherwise(col("LAST_M_PROD_SALES_VALUE") / col("LAST_M_PROV_SALES_VALUE")))
            .withColumn("LAST_M_PROD_IN_MKT_SHARE", when(col("LAST_M_MKT_SALES_VALUE") === 0.0, 0.0).otherwise(col("LAST_M_PROD_SALES_VALUE") / col("LAST_M_MKT_SALES_VALUE")))
            .withColumn("LAST_M_CITY_IN_PROV_SHARE", when(col("LAST_M_PROV_SALES_VALUE") === 0.0, 0.0).otherwise(col("LAST_M_CITY_SALES_VALUE") / col("LAST_M_PROV_SALES_VALUE")))
            .withColumn("LAST_M_CITY_IN_MKT_SHARE", when(col("LAST_M_MKT_SALES_VALUE") === 0.0, 0.0).otherwise(col("LAST_M_CITY_SALES_VALUE") / col("LAST_M_MKT_SALES_VALUE")))
            .withColumn("LAST_M_PROV_IN_MKT_SHARE", when(col("LAST_M_MKT_SALES_VALUE") === 0.0, 0.0).otherwise(col("LAST_M_PROV_SALES_VALUE") / col("LAST_M_MKT_SALES_VALUE")))
            .withColumn("MOLE_MOM", when(col("LAST_M_MOLE_SALES_VALUE") === 0.0, 0.0).otherwise((col("MOLE_SALES_VALUE") - col("LAST_M_MOLE_SALES_VALUE")) / col("LAST_M_MOLE_SALES_VALUE")))
            .withColumn("PROD_MOM", when(col("LAST_M_PROD_SALES_VALUE") === 0.0, 0.0).otherwise((col("PROD_SALES_VALUE") - col("LAST_M_PROD_SALES_VALUE")) / col("LAST_M_PROD_SALES_VALUE")))
            .withColumn("CITY_MOM", when(col("LAST_M_CITY_SALES_VALUE") === 0.0, 0.0).otherwise((col("CITY_SALES_VALUE") - col("LAST_M_CITY_SALES_VALUE")) / col("LAST_M_CITY_SALES_VALUE")))
            .withColumn("PROV_MOM", when(col("LAST_M_PROV_SALES_VALUE") === 0.0, 0.0).otherwise((col("PROV_SALES_VALUE") - col("LAST_M_PROV_SALES_VALUE")) / col("LAST_M_PROV_SALES_VALUE")))
            .withColumn("MKT_MOM", when(col("LAST_M_MKT_SALES_VALUE") === 0.0, 0.0).otherwise((col("MKT_SALES_VALUE") - col("LAST_M_MKT_SALES_VALUE")) / col("LAST_M_MKT_SALES_VALUE")))
            .withColumn("EI", when(col("LAST_M_PROD_IN_MKT_SHARE") === 0.0, 0.0).otherwise(col("PROD_IN_MKT_SHARE") / col("LAST_M_PROD_IN_MKT_SHARE")))
            //fill lastYear values
            .withColumn("LAST_Y_MOLE_SALES_VALUE", first("MOLE_SALES_VALUE").over(lastYearWindow(moleWindow)))
            .na.fill(Map("LAST_Y_MOLE_SALES_VALUE" -> 0.0))
            .withColumn("LAST_Y_PROD_SALES_VALUE", first("PROD_SALES_VALUE").over(lastYearWindow(prodWindow)))
            .na.fill(Map("LAST_Y_PROD_SALES_VALUE" -> 0.0))
            .withColumn("LAST_Y_CITY_SALES_VALUE", first("CITY_SALES_VALUE").over(lastYearWindow(cityWindow)))
            .na.fill(Map("LAST_Y_CITY_SALES_VALUE" -> 0.0))
            .withColumn("LAST_Y_PROV_SALES_VALUE", first("PROV_SALES_VALUE").over(lastYearWindow(provWindow)))
            .na.fill(Map("LAST_Y_PROV_SALES_VALUE" -> 0.0))
            .withColumn("LAST_Y_MKT_SALES_VALUE", first("MKT_SALES_VALUE").over(lastYearWindow(mktWindow)))
            .na.fill(Map("LAST_Y_MKT_SALES_VALUE" -> 0.0))
            .withColumn("LAST_Y_MOLE_IN_PROD_SHARE", when(col("LAST_Y_PROD_SALES_VALUE") === 0.0, 0.0).otherwise(col("LAST_Y_MOLE_SALES_VALUE") / col("LAST_Y_PROD_SALES_VALUE")))
            .withColumn("LAST_Y_MOLE_IN_CITY_SHARE", when(col("LAST_Y_CITY_SALES_VALUE") === 0.0, 0.0).otherwise(col("LAST_Y_MOLE_SALES_VALUE") / col("LAST_Y_CITY_SALES_VALUE")))
            .withColumn("LAST_Y_MOLE_IN_PROV_SHARE", when(col("LAST_Y_PROV_SALES_VALUE") === 0.0, 0.0).otherwise(col("LAST_Y_MOLE_SALES_VALUE") / col("LAST_Y_PROV_SALES_VALUE")))
            .withColumn("LAST_Y_MOLE_IN_MKT_SHARE", when(col("LAST_Y_MKT_SALES_VALUE") === 0.0, 0.0).otherwise(col("LAST_Y_MOLE_SALES_VALUE") / col("LAST_Y_MKT_SALES_VALUE")))
            .withColumn("LAST_Y_PROD_IN_CITY_SHARE", when(col("LAST_Y_CITY_SALES_VALUE") === 0.0, 0.0).otherwise(col("LAST_Y_PROD_SALES_VALUE") / col("LAST_Y_CITY_SALES_VALUE")))
            .withColumn("LAST_Y_PROD_IN_PROV_SHARE", when(col("LAST_Y_PROV_SALES_VALUE") === 0.0, 0.0).otherwise(col("LAST_Y_PROD_SALES_VALUE") / col("LAST_Y_PROV_SALES_VALUE")))
            .withColumn("LAST_Y_PROD_IN_MKT_SHARE", when(col("LAST_Y_MKT_SALES_VALUE") === 0.0, 0.0).otherwise(col("LAST_Y_PROD_SALES_VALUE") / col("LAST_Y_MKT_SALES_VALUE")))
            .withColumn("LAST_Y_CITY_IN_PROV_SHARE", when(col("LAST_Y_PROV_SALES_VALUE") === 0.0, 0.0).otherwise(col("LAST_Y_CITY_SALES_VALUE") / col("LAST_Y_PROV_SALES_VALUE")))
            .withColumn("LAST_Y_CITY_IN_MKT_SHARE", when(col("LAST_Y_MKT_SALES_VALUE") === 0.0, 0.0).otherwise(col("LAST_Y_CITY_SALES_VALUE") / col("LAST_Y_MKT_SALES_VALUE")))
            .withColumn("LAST_Y_PROV_IN_MKT_SHARE", when(col("LAST_Y_MKT_SALES_VALUE") === 0.0, 0.0).otherwise(col("LAST_Y_PROV_SALES_VALUE") / col("LAST_Y_MKT_SALES_VALUE")))
            .withColumn("MOLE_YOY", when(col("LAST_Y_MOLE_SALES_VALUE") === 0.0, 0.0).otherwise((col("MOLE_SALES_VALUE") - col("LAST_Y_MOLE_SALES_VALUE")) / col("LAST_Y_MOLE_SALES_VALUE")))
            .withColumn("PROD_YOY", when(col("LAST_Y_PROD_SALES_VALUE") === 0.0, 0.0).otherwise((col("PROD_SALES_VALUE") - col("LAST_Y_PROD_SALES_VALUE")) / col("LAST_Y_PROD_SALES_VALUE")))
            .withColumn("CITY_YOY", when(col("LAST_Y_CITY_SALES_VALUE") === 0.0, 0.0).otherwise((col("CITY_SALES_VALUE") - col("LAST_Y_CITY_SALES_VALUE")) / col("LAST_Y_CITY_SALES_VALUE")))
            .withColumn("PROV_YOY", when(col("LAST_Y_PROV_SALES_VALUE") === 0.0, 0.0).otherwise((col("PROV_SALES_VALUE") - col("LAST_Y_PROV_SALES_VALUE")) / col("LAST_Y_PROV_SALES_VALUE")))
            .withColumn("MKT_YOY", when(col("LAST_Y_MKT_SALES_VALUE") === 0.0, 0.0).otherwise((col("MKT_SALES_VALUE") - col("LAST_Y_MKT_SALES_VALUE")) / col("LAST_Y_MKT_SALES_VALUE")))

    }

    def currMonthWindow(window: WindowSpec): WindowSpec = {
        window.orderBy(col("YM").cast(DataTypes.IntegerType)).rangeBetween(0, 0)
    }

    def lastMonthWindow(window: WindowSpec): WindowSpec = {
        window
            .orderBy(to_date(col("YM").cast("string"), "yyyyMM").cast("timestamp").cast("long"))
            .rangeBetween(-86400 * 31, -86400 * 28)
    }


    def lastYearWindow(window: WindowSpec): WindowSpec =
        window.orderBy(col("YM").cast(DataTypes.IntegerType)).rangeBetween(-100, -100)

}
