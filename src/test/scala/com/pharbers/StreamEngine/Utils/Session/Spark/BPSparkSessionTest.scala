package com.pharbers.StreamEngine.Utils.Session.Spark

import com.pharbers.util.log.PhLogable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.{UnboundedFollowing, UnboundedPreceding}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.scalatest.FunSuite

/** Spark Session Test
 *
 * @author clock
 * @version 0.0.1
 * @since 2019/11/06 10:04
 * @note 注意配置文件的优先级和使用方式
 */
class BPSparkSessionTest extends FunSuite with PhLogable {
    test("Create BPSparkSession By Default Config") {
        assert(BPSparkSession() != null)
    }

    test("Create BPSparkSession By Args Config") {
        assert(BPSparkSession(Map("app.name" -> "BPSparkSessionTest")) != null)
    }

    test("Read Hive And Convert MaxDashboard") {
        val spark = BPSparkSession(Map("app.name" -> "ReadHive"))
        //        val reading = spark.sql("SELECT COMPANY, SOURCE, PROVINCE_NAME, CITY_NAME, HOSP_NAME, HOSP_CODE, CAST(SALES_VALUE As DOUBLE) AS SALES, CAST(YEAR As INT) AS YEAR, CAST(MONTH As INT) AS MONTH" +

        val reading = spark.sql("SELECT *" +
            " FROM cpa WHERE ( YEAR >= 2017 ) AND ( YEAR <= 2019 ) AND ( MONTH >= 1 ) AND ( MONTH <= 12 )")
        //        println("origin count = ", reading.count())
        val newData = convert(reading)
        //        println("new count = ", newData.count())

        //        newData.show(10)
        newData.printSchema()
        //        newData.groupBy("YM").agg("SALES_VALUE" -> "sum").show(100)
    }

    def convert(data: DataFrame): DataFrame = {
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
            .withColumn("YM", col("YEAR").*(100).+(col("MONTH")))
            .withColumn("SALES_QTY", col("SALES_QTY").cast(DataTypes.LongType))
            .withColumn("SALES_VALUE", col("SALES_VALUE").cast(DataTypes.DoubleType))

        //缩小数据范围，需求中最小维度是分子，先计算出分子级别在单个年月、省&城市级别、产品维度的聚合数据
        val moleLevelDF = formatDF.groupBy("YM", "PROVINCE_NAME", "CITY_NAME", "PRODUCT_NAME", "MOLE_NAME")
            .agg(expr("SUM(SALES_QTY) as MOLE_SALES_QTY"), expr("SUM(SALES_VALUE) as MOLE_SALES_VALUE"))

        //TODO:得保证数据源中包含两年的数据
        val current2YearYmList = moleLevelDF.select("YM").distinct().sort("YM").collect().map(_(0)).toList.takeRight(24).asInstanceOf[List[Int]]

        val moleWindow = Window
            .partitionBy("YM", "PROVINCE_NAME", "CITY_NAME", "PRODUCT_NAME", "MOLE_NAME")

        val lastMonthMoleWindow = moleWindow
            .orderBy(to_date(col("YM").cast("string"), "yyyyMM").cast("timestamp").cast("long"))
            .rangeBetween(-86400 * 31, Window.currentRow)

        val prodWindow = Window
            .partitionBy("YM", "PROVINCE_NAME", "CITY_NAME", "PRODUCT_NAME")

        val cityWindow = Window
            .partitionBy("YM", "PROVINCE_NAME", "CITY_NAME")

        val provWindow = Window
            .partitionBy("YM", "PROVINCE_NAME")

        val ymWindow = Window
            .partitionBy("YM")

        val current2YearDF = moleLevelDF
            .filter(col("YM") >= current2YearYmList.min.toString.toInt && col("YM") <= current2YearYmList.max.toString.toInt)
//            .withColumn("PROD_SALES_QTY", sum("MOLE_SALES_QTY").over(prodWindow))
//            .withColumn("PROD_SALES_VALUE", sum("MOLE_SALES_VALUE").over(prodWindow))
//            .withColumn("CITY_SALES_QTY", sum("MOLE_SALES_QTY").over(cityWindow))
//            .withColumn("CITY_SALES_VALUE", sum("MOLE_SALES_VALUE").over(cityWindow))
//            .withColumn("PROV_SALES_QTY", sum("MOLE_SALES_QTY").over(provWindow))
//            .withColumn("PROV_SALES_VALUE", sum("MOLE_SALES_VALUE").over(provWindow))
//            .withColumn("YM_SALES_QTY", sum("MOLE_SALES_QTY").over(ymWindow))
//            .withColumn("YM_SALES_VALUE", sum("MOLE_SALES_VALUE").over(ymWindow))
            //fill lastMonth values
            .withColumn("LAST_M_MOLE_SALES_QTY", sum("MOLE_SALES_QTY").over(lastMonthMoleWindow) - col("MOLE_SALES_QTY"))
            .withColumn("LAST_M_MOLE_SALES_VALUE", sum("MOLE_SALES_VALUE").over(lastMonthMoleWindow) - col("MOLE_SALES_VALUE"))
            .na.fill(Map("LAST_M_MOLE_SALES_QTY" -> 0, "LAST_M_MOLE_SALES_VALUE" -> 0.0))
//            .withColumn("LAST_M_PROD_SALES_QTY", first("PROD_SALES_QTY").over(lastMonthWindow(prodWindow)))
//            .withColumn("LAST_M_PROD_SALES_VALUE", first("PROD_SALES_VALUE").over(lastMonthWindow(prodWindow)))
//            .na.fill(Map("LAST_M_PROD_SALES_QTY" -> 0, "LAST_M_PROD_SALES_VALUE" -> 0.0))
//            .withColumn("LAST_M_CITY_SALES_QTY", first("CITY_SALES_QTY").over(lastMonthWindow(cityWindow)))
//            .withColumn("LAST_M_CITY_SALES_VALUE", first("CITY_SALES_VALUE").over(lastMonthWindow(cityWindow)))
//            .na.fill(Map("LAST_M_CITY_SALES_QTY" -> 0, "LAST_M_CITY_SALES_VALUE" -> 0.0))
//            .withColumn("LAST_M_PROV_SALES_QTY", first("PROV_SALES_QTY").over(lastMonthWindow(provWindow)))
//            .withColumn("LAST_M_PROV_SALES_VALUE", first("PROV_SALES_VALUE").over(lastMonthWindow(provWindow)))
//            .na.fill(Map("LAST_M_PROV_SALES_QTY" -> 0, "LAST_M_PROV_SALES_VALUE" -> 0.0))
//            .withColumn("LAST_M_YM_SALES_QTY", first("YM_SALES_QTY").over(lastMonthWindow(ymWindow)))
//            .withColumn("LAST_M_YM_SALES_VALUE", first("YM_SALES_VALUE").over(lastMonthWindow(ymWindow)))
//            .na.fill(Map("LAST_M_YM_SALES_QTY" -> 0, "LAST_M_YM_SALES_VALUE" -> 0.0))
//            //fill lastYear values
//            .withColumn("LAST_Y_MOLE_SALES_QTY", first("MOLE_SALES_QTY").over(lastYearWindow(moleWindow)))
//            .withColumn("LAST_Y_MOLE_SALES_VALUE", first("MOLE_SALES_VALUE").over(lastYearWindow(moleWindow)))
//            .na.fill(Map("LAST_Y_MOLE_SALES_QTY" -> 0, "LAST_Y_MOLE_SALES_VALUE" -> 0.0))
//            .withColumn("LAST_Y_PROD_SALES_QTY", first("PROD_SALES_QTY").over(lastYearWindow(prodWindow)))
//            .withColumn("LAST_Y_PROD_SALES_VALUE", first("PROD_SALES_VALUE").over(lastYearWindow(prodWindow)))
//            .na.fill(Map("LAST_Y_PROD_SALES_QTY" -> 0, "LAST_Y_PROD_SALES_VALUE" -> 0.0))
//            .withColumn("LAST_Y_CITY_SALES_QTY", first("CITY_SALES_QTY").over(lastYearWindow(cityWindow)))
//            .withColumn("LAST_Y_CITY_SALES_VALUE", first("CITY_SALES_VALUE").over(lastYearWindow(cityWindow)))
//            .na.fill(Map("LAST_Y_CITY_SALES_QTY" -> 0, "LAST_Y_CITY_SALES_VALUE" -> 0.0))
//            .withColumn("LAST_Y_PROV_SALES_QTY", first("PROV_SALES_QTY").over(lastYearWindow(provWindow)))
//            .withColumn("LAST_Y_PROV_SALES_VALUE", first("PROV_SALES_VALUE").over(lastYearWindow(provWindow)))
//            .na.fill(Map("LAST_Y_PROV_SALES_QTY" -> 0, "LAST_Y_PROV_SALES_VALUE" -> 0.0))
//            .withColumn("LAST_Y_YM_SALES_QTY", first("YM_SALES_QTY").over(lastYearWindow(ymWindow)))
//            .withColumn("LAST_Y_YM_SALES_VALUE", first("YM_SALES_VALUE").over(lastYearWindow(ymWindow)))
//            .na.fill(Map("LAST_Y_YM_SALES_QTY" -> 0, "LAST_Y_YM_SALES_VALUE" -> 0.0))

        current2YearDF.show(20)
//        current2YearDF.filter(col("PRODUCT_NAME") === "丽泉" && col("CITY_NAME") === "七台河市" && col("YM") === "201803").show(10)
        return current2YearDF
    }

    def lastMonthWindow(window: WindowSpec): WindowSpec = {
        window
            .orderBy(to_date(col("YM").cast("string"), "yyyyMM").cast("timestamp").cast("long"))
            .rangeBetween(-86400 * 31, -86400 * 28)
    }


    def lastYearWindow(window: WindowSpec): WindowSpec =
        window.orderBy(col("YM").cast(DataTypes.IntegerType)).rangeBetween(-100, -100)

}
