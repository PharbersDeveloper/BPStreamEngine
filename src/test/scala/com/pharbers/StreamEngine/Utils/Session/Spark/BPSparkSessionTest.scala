package com.pharbers.StreamEngine.Utils.Session.Spark

import com.pharbers.util.log.PhLogable
import org.apache.spark.sql.DataFrame
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

        //分子级别在单个年月、省&城市级别、产品维度的聚合数据
        val moleLevelDF = formatDF.groupBy("YM", "PROVINCE_NAME", "CITY_NAME", "PRODUCT_NAME", "MOLE_NAME")
            .agg(expr("SUM(SALES_QTY) as MOLE_SALES_QTY"), expr("SUM(SALES_VALUE) as MOLE_SALES_VALUE"))

        //TODO:得保证数据源中包含两年的数据
        val current2YearYmList = moleLevelDF.select("YM").sort("YM").distinct().collect().map(_(0)).toList.takeRight(24)
        val currentYearYmList = current2YearYmList.takeRight(12).asInstanceOf[List[Int]]
        val lastYearYmList = current2YearYmList.take(12).asInstanceOf[List[Int]]
        val lastMonthYmList = current2YearYmList.takeRight(13).take(12).asInstanceOf[List[Int]]

        //当前最新的12个月的各分组（分子、产品、城市、省份、年月）数据
        val currentYearMoleLevelDF = moleLevelDF
            .filter(col("YM") >= currentYearYmList.min.toString.toInt && col("YM") <= currentYearYmList.max.toString.toInt)
            .sort("YM", "PROVINCE_NAME", "CITY_NAME", "PRODUCT_NAME", "MOLE_NAME")
            .withColumnRenamed("MOLE_SALES_QTY", "CURR_MOLE_SALES_QTY")
            .withColumnRenamed("MOLE_SALES_VALUE", "CURR_MOLE_SALES_VALUE")

        val currentYearProductLevelDF = currentYearMoleLevelDF.groupBy("YM", "PROVINCE_NAME", "CITY_NAME", "PRODUCT_NAME")
            .agg(expr("SUM(CURR_MOLE_SALES_QTY) as CURR_PROD_SALES_QTY"), expr("SUM(CURR_MOLE_SALES_VALUE) as CURR_PROD_SALES_VALUE"))
            .withColumnRenamed("YM", "CURR_YM")

        val currentYearCityLevelDF = currentYearProductLevelDF.groupBy("CURR_YM", "PROVINCE_NAME", "CITY_NAME")
            .agg(expr("SUM(CURR_PROD_SALES_QTY) as CURR_CITY_SALES_QTY"), expr("SUM(CURR_PROD_SALES_VALUE) as CURR_CITY_SALES_VALUE"))

        val currentYearProvinceLevelDF = currentYearCityLevelDF.groupBy("CURR_YM", "PROVINCE_NAME")
            .agg(expr("SUM(CURR_CITY_SALES_QTY) as CURR_PROV_SALES_QTY"), expr("SUM(CURR_CITY_SALES_VALUE) as CURR_PROV_SALES_VALUE"))

        val currentYearYmLevelDF = currentYearProvinceLevelDF.groupBy("CURR_YM")
            .agg(expr("SUM(CURR_PROV_SALES_QTY) as CURR_YM_SALES_QTY"), expr("SUM(CURR_PROV_SALES_VALUE) as CURR_YM_SALES_VALUE"))

        //去年（12个月前）的12个月的各分组（分子、产品、城市、省份、年月）数据
        val lastYearMoleLevelDF = moleLevelDF
            .filter(col("YM") >= lastYearYmList.min.toString.toInt && col("YM") <= lastYearYmList.max.toString.toInt)
            .sort("YM", "PROVINCE_NAME", "CITY_NAME", "PRODUCT_NAME", "MOLE_NAME")
            .withColumnRenamed("MOLE_SALES_QTY", "LAST_Y_MOLE_SALES_QTY")
            .withColumnRenamed("MOLE_SALES_VALUE", "LAST_Y_MOLE_SALES_VALUE")
            .withColumnRenamed("YM", "LAST_YEAR_YM")
            .withColumn("CURR_YEAR_YM", col("LAST_YEAR_YM").+(100))

        val lastYearProductLevelDF = lastYearMoleLevelDF.groupBy("LAST_YEAR_YM", "CURR_YEAR_YM", "PROVINCE_NAME", "CITY_NAME", "PRODUCT_NAME")
            .agg(expr("SUM(LAST_Y_MOLE_SALES_QTY) as LAST_Y_PROD_SALES_QTY"), expr("SUM(LAST_Y_MOLE_SALES_VALUE) as LAST_Y_PROD_SALES_VALUE"))

        val lastYearCityLevelDF = lastYearProductLevelDF.groupBy("LAST_YEAR_YM", "CURR_YEAR_YM", "PROVINCE_NAME", "CITY_NAME")
            .agg(expr("SUM(LAST_Y_PROD_SALES_QTY) as LAST_Y_CITY_SALES_QTY"), expr("SUM(LAST_Y_PROD_SALES_VALUE) as LAST_Y_CITY_SALES_VALUE"))

        val lastYearProvinceLevelDF = lastYearCityLevelDF.groupBy("LAST_YEAR_YM", "CURR_YEAR_YM", "PROVINCE_NAME")
            .agg(expr("SUM(LAST_Y_CITY_SALES_QTY) as LAST_Y_PROV_SALES_QTY"), expr("SUM(LAST_Y_CITY_SALES_VALUE) as LAST_Y_PROV_SALES_VALUE"))

        val lastYearYmLevelDF = lastYearProvinceLevelDF.groupBy("LAST_YEAR_YM", "CURR_YEAR_YM")
            .agg(expr("SUM(LAST_Y_PROV_SALES_QTY) as LAST_Y_YM_SALES_QTY"), expr("SUM(LAST_Y_PROV_SALES_VALUE) as LAST_Y_YM_SALES_VALUE"))

        //上月（1个月前）的12个月的各分组（分子、产品、城市、省份、年月）数据
        val lastMonthYearMoleLevelDF = moleLevelDF
            .filter(col("YM") >= lastMonthYmList.min.toString.toInt && col("YM") <= lastMonthYmList.max.toString.toInt)
            .sort("YM", "PROVINCE_NAME", "CITY_NAME", "PRODUCT_NAME", "MOLE_NAME")
            .withColumnRenamed("MOLE_SALES_QTY", "LAST_M_MOLE_SALES_QTY")
            .withColumnRenamed("MOLE_SALES_VALUE", "LAST_M_MOLE_SALES_VALUE")
            .withColumnRenamed("YM", "LAST_MONTH_YM")
            .withColumn("CURR_MONTH_YM", when(col("LAST_MONTH_YM") === 12, col("LAST_MONTH_YM").+(89)).otherwise(col("LAST_MONTH_YM").+(1)))

        val lastMonthProductLevelDF = lastMonthYearMoleLevelDF.groupBy("LAST_MONTH_YM", "CURR_MONTH_YM", "PROVINCE_NAME", "CITY_NAME", "PRODUCT_NAME")
            .agg(expr("SUM(LAST_M_MOLE_SALES_QTY) as LAST_M_PROD_SALES_QTY"), expr("SUM(LAST_M_MOLE_SALES_VALUE) as LAST_M_PROD_SALES_VALUE"))

        val lastMonthCityLevelDF = lastMonthProductLevelDF.groupBy("LAST_MONTH_YM", "CURR_MONTH_YM", "PROVINCE_NAME", "CITY_NAME")
            .agg(expr("SUM(LAST_M_PROD_SALES_QTY) as LAST_M_CITY_SALES_QTY"), expr("SUM(LAST_M_PROD_SALES_VALUE) as LAST_M_CITY_SALES_VALUE"))

        val lastMonthProvinceLevelDF = lastMonthCityLevelDF.groupBy("LAST_MONTH_YM", "CURR_MONTH_YM", "PROVINCE_NAME")
            .agg(expr("SUM(LAST_M_CITY_SALES_QTY) as LAST_M_PROV_SALES_QTY"), expr("SUM(LAST_M_CITY_SALES_VALUE) as LAST_M_PROV_SALES_VALUE"))

        val lastMonthYmLevelDF = lastMonthProvinceLevelDF.groupBy("LAST_MONTH_YM", "CURR_MONTH_YM")
            .agg(expr("SUM(LAST_M_PROV_SALES_QTY) as LAST_M_YM_SALES_QTY"), expr("SUM(LAST_M_PROV_SALES_VALUE) as LAST_M_YM_SALES_VALUE"))

        val mergedDF = currentYearMoleLevelDF
            .join(lastYearMoleLevelDF,
                currentYearMoleLevelDF("YM") === lastYearMoleLevelDF("CURR_YEAR_YM") &&
                currentYearMoleLevelDF("PROVINCE_NAME") === lastYearMoleLevelDF("PROVINCE_NAME") &&
                currentYearMoleLevelDF("CITY_NAME") === lastYearMoleLevelDF("CITY_NAME") &&
                currentYearMoleLevelDF("PRODUCT_NAME") === lastYearMoleLevelDF("PRODUCT_NAME") &&
                currentYearMoleLevelDF("MOLE_NAME") === lastYearMoleLevelDF("MOLE_NAME"),
                "left")
            .drop(lastYearMoleLevelDF("PROVINCE_NAME"))
            .drop(lastYearMoleLevelDF("CITY_NAME"))
            .drop(lastYearMoleLevelDF("PRODUCT_NAME"))
            .drop(lastYearMoleLevelDF("MOLE_NAME"))
            .drop(lastYearMoleLevelDF("LAST_YEAR_YM"))
            .drop(lastYearMoleLevelDF("CURR_YEAR_YM"))
            .join(lastMonthYearMoleLevelDF,
                currentYearMoleLevelDF("YM") === lastMonthYearMoleLevelDF("CURR_MONTH_YM") &&
                    currentYearMoleLevelDF("PROVINCE_NAME") === lastMonthYearMoleLevelDF("PROVINCE_NAME") &&
                    currentYearMoleLevelDF("CITY_NAME") === lastMonthYearMoleLevelDF("CITY_NAME") &&
                    currentYearMoleLevelDF("PRODUCT_NAME") === lastMonthYearMoleLevelDF("PRODUCT_NAME") &&
                    currentYearMoleLevelDF("MOLE_NAME") === lastMonthYearMoleLevelDF("MOLE_NAME"),
                "left")
            .drop(lastMonthYearMoleLevelDF("PROVINCE_NAME"))
            .drop(lastMonthYearMoleLevelDF("CITY_NAME"))
            .drop(lastMonthYearMoleLevelDF("PRODUCT_NAME"))
            .drop(lastMonthYearMoleLevelDF("MOLE_NAME"))
            .drop(lastMonthYearMoleLevelDF("LAST_MONTH_YM"))
            .drop(lastMonthYearMoleLevelDF("CURR_MONTH_YM"))
            .join(currentYearProductLevelDF,
                currentYearMoleLevelDF("YM") === currentYearProductLevelDF("CURR_YM") &&
                    currentYearMoleLevelDF("PROVINCE_NAME") === currentYearProductLevelDF("PROVINCE_NAME") &&
                    currentYearMoleLevelDF("CITY_NAME") === currentYearProductLevelDF("CITY_NAME") &&
                    currentYearMoleLevelDF("PRODUCT_NAME") === currentYearProductLevelDF("PRODUCT_NAME"),
                "left")
            .drop(currentYearProductLevelDF("PROVINCE_NAME"))
            .drop(currentYearProductLevelDF("CITY_NAME"))
            .drop(currentYearProductLevelDF("PRODUCT_NAME"))
            .drop(currentYearProductLevelDF("CURR_YM"))
            .join(lastYearProductLevelDF,
                currentYearMoleLevelDF("YM") === lastYearProductLevelDF("CURR_YEAR_YM") &&
                    currentYearMoleLevelDF("PROVINCE_NAME") === lastYearProductLevelDF("PROVINCE_NAME") &&
                    currentYearMoleLevelDF("CITY_NAME") === lastYearProductLevelDF("CITY_NAME") &&
                    currentYearMoleLevelDF("PRODUCT_NAME") === lastYearProductLevelDF("PRODUCT_NAME"),
                "left")
            .drop(lastYearProductLevelDF("PROVINCE_NAME"))
            .drop(lastYearProductLevelDF("CITY_NAME"))
            .drop(lastYearProductLevelDF("PRODUCT_NAME"))
            .drop(lastYearProductLevelDF("LAST_YEAR_YM"))
            .drop(lastYearProductLevelDF("CURR_YEAR_YM"))
            .join(lastMonthProductLevelDF,
                currentYearMoleLevelDF("YM") === lastMonthProductLevelDF("CURR_MONTH_YM") &&
                    currentYearMoleLevelDF("PROVINCE_NAME") === lastMonthProductLevelDF("PROVINCE_NAME") &&
                    currentYearMoleLevelDF("CITY_NAME") === lastMonthProductLevelDF("CITY_NAME") &&
                    currentYearMoleLevelDF("PRODUCT_NAME") === lastMonthProductLevelDF("PRODUCT_NAME"),
                "left")
            .drop(lastMonthProductLevelDF("PROVINCE_NAME"))
            .drop(lastMonthProductLevelDF("CITY_NAME"))
            .drop(lastMonthProductLevelDF("PRODUCT_NAME"))
            .drop(lastMonthProductLevelDF("LAST_MONTH_YM"))
            .drop(lastMonthProductLevelDF("CURR_MONTH_YM"))
            .join(currentYearCityLevelDF,
                currentYearMoleLevelDF("YM") === currentYearCityLevelDF("CURR_YM") &&
                    currentYearMoleLevelDF("PROVINCE_NAME") === currentYearCityLevelDF("PROVINCE_NAME") &&
                    currentYearMoleLevelDF("CITY_NAME") === currentYearCityLevelDF("CITY_NAME"),
                "left")
            .drop(currentYearCityLevelDF("PROVINCE_NAME"))
            .drop(currentYearCityLevelDF("CITY_NAME"))
            .drop(currentYearCityLevelDF("CURR_YM"))
            .join(lastYearCityLevelDF,
                currentYearMoleLevelDF("YM") === lastYearCityLevelDF("CURR_YEAR_YM") &&
                    currentYearMoleLevelDF("PROVINCE_NAME") === lastYearCityLevelDF("PROVINCE_NAME") &&
                    currentYearMoleLevelDF("CITY_NAME") === lastYearCityLevelDF("CITY_NAME"),
                "left")
            .drop(lastYearCityLevelDF("PROVINCE_NAME"))
            .drop(lastYearCityLevelDF("CITY_NAME"))
            .drop(lastYearCityLevelDF("LAST_YEAR_YM"))
            .drop(lastYearCityLevelDF("CURR_YEAR_YM"))
            .join(lastMonthCityLevelDF,
                currentYearMoleLevelDF("YM") === lastMonthCityLevelDF("CURR_MONTH_YM") &&
                    currentYearMoleLevelDF("PROVINCE_NAME") === lastMonthCityLevelDF("PROVINCE_NAME") &&
                    currentYearMoleLevelDF("CITY_NAME") === lastMonthCityLevelDF("CITY_NAME"),
                "left")
            .drop(lastMonthCityLevelDF("PROVINCE_NAME"))
            .drop(lastMonthCityLevelDF("CITY_NAME"))
            .drop(lastMonthCityLevelDF("LAST_MONTH_YM"))
            .drop(lastMonthCityLevelDF("CURR_MONTH_YM"))
            .join(currentYearProvinceLevelDF,
                currentYearMoleLevelDF("YM") === currentYearProvinceLevelDF("CURR_YM") &&
                    currentYearMoleLevelDF("PROVINCE_NAME") === currentYearProvinceLevelDF("PROVINCE_NAME"),
                "left")
            .drop(currentYearProvinceLevelDF("PROVINCE_NAME"))
            .drop(currentYearProvinceLevelDF("CURR_YM"))
            .join(lastYearProvinceLevelDF,
                currentYearMoleLevelDF("YM") === lastYearProvinceLevelDF("CURR_YEAR_YM") &&
                    currentYearMoleLevelDF("PROVINCE_NAME") === lastYearProvinceLevelDF("PROVINCE_NAME"),
                "left")
            .drop(lastYearProvinceLevelDF("PROVINCE_NAME"))
            .drop(lastYearProvinceLevelDF("LAST_YEAR_YM"))
            .drop(lastYearProvinceLevelDF("CURR_YEAR_YM"))
            .join(lastMonthProvinceLevelDF,
                currentYearMoleLevelDF("YM") === lastMonthProvinceLevelDF("CURR_MONTH_YM") &&
                    currentYearMoleLevelDF("PROVINCE_NAME") === lastMonthProvinceLevelDF("PROVINCE_NAME"),
                "left")
            .drop(lastMonthProvinceLevelDF("PROVINCE_NAME"))
            .drop(lastMonthProvinceLevelDF("LAST_MONTH_YM"))
            .drop(lastMonthProvinceLevelDF("CURR_MONTH_YM"))
            .join(currentYearYmLevelDF,
                currentYearMoleLevelDF("YM") === currentYearYmLevelDF("CURR_YM"),
                "left")
            .drop(currentYearYmLevelDF("CURR_YM"))
            .join(lastYearYmLevelDF,
                currentYearMoleLevelDF("YM") === lastYearYmLevelDF("CURR_YEAR_YM"),
                "left")
            .drop(lastYearYmLevelDF("CURR_YEAR_YM"))
            .join(lastMonthYmLevelDF,
                currentYearMoleLevelDF("YM") === lastMonthYmLevelDF("CURR_MONTH_YM"),
                "left")
            .drop(lastMonthYmLevelDF("CURR_MONTH_YM"))

        return mergedDF
    }

}
