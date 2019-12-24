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

        //分子级别在单个年月、省&城市级别、产品维度的聚合数据
        val moleLevelDF = formatDF.groupBy("YM", "PROVINCE_NAME", "CITY_NAME", "PRODUCT_NAME", "MOLE_NAME")
            .agg(expr("SUM(SALES_QTY) as MOLE_SALES_QTY"), expr("SUM(SALES_VALUE) as MOLE_SALES_VALUE"))

        //TODO:得保证数据源中包含两年的数据
        val current2YearYmList = moleLevelDF.select("YM").sort("YM").distinct().collect().map(_(0)).toList.takeRight(24).asInstanceOf[List[Int]]
        val currentYearYmList = current2YearYmList.takeRight(12)
        val lastMonthYmList = current2YearYmList.takeRight(13).take(12)

        //当前最新的12个月的各分组（分子、产品、城市、省份、年月）数据
        val currentYearMoleLevelDF = moleLevelDF
            .filter(col("YM") >= currentYearYmList.min.toString.toInt && col("YM") <= currentYearYmList.max.toString.toInt)
            .sort("YM", "PROVINCE_NAME", "CITY_NAME", "PRODUCT_NAME", "MOLE_NAME")
            .withColumnRenamed("MOLE_SALES_QTY", "CURR_MOLE_SALES_QTY")
            .withColumnRenamed("MOLE_SALES_VALUE", "CURR_MOLE_SALES_VALUE")

        //上月（1个月前）的12个月的各分组（分子、产品、城市、省份、年月）数据
        val lastMonthYearMoleLevelDF = moleLevelDF
            .filter(col("YM") >= lastMonthYmList.min.toString.toInt && col("YM") <= lastMonthYmList.max.toString.toInt)
            .sort("YM", "PROVINCE_NAME", "CITY_NAME", "PRODUCT_NAME", "MOLE_NAME")
            .withColumnRenamed("MOLE_SALES_QTY", "LAST_M_MOLE_SALES_QTY")
            .withColumnRenamed("MOLE_SALES_VALUE", "LAST_M_MOLE_SALES_VALUE")
            .withColumnRenamed("YM", "LAST_MONTH_YM")
            .withColumn("CURR_MONTH_YM", when(col("LAST_MONTH_YM") === 12, col("LAST_MONTH_YM").+(89)).otherwise(col("LAST_MONTH_YM").+(1)))

        lastMonthYearMoleLevelDF.show(10)

        val current2YearMoleLevelDF = moleLevelDF
            .filter(col("YM") >= current2YearYmList.min.toString.toInt && col("YM") <= current2YearYmList.max.toString.toInt)
            .sort("YM", "PROVINCE_NAME", "CITY_NAME", "PRODUCT_NAME", "MOLE_NAME")

        val lastMonthMoleWindow = Window
            .partitionBy("YM", "PROVINCE_NAME", "CITY_NAME", "PRODUCT_NAME", "MOLE_NAME")
            .orderBy("YM")
            .rowsBetween(-1, 0)

        current2YearMoleLevelDF
            .withColumn("LAST_M_MOLE_SALES_QTY", first(col("MOLE_SALES_QTY").over(lastMonthMoleWindow)))
            .withColumn("LAST_M_MOLE_SALES_VALUE", first(col("MOLE_SALES_VALUE").over(lastMonthMoleWindow)))

        current2YearMoleLevelDF.show(10)

        return moleLevelDF
    }

}
