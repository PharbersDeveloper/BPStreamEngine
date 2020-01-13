package com.pharbers.StreamEngine.Utils.Session.Spark

import com.pharbers.util.log.PhLogable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{UnboundedFollowing, UnboundedPreceding}
import org.apache.spark.sql.catalyst.plans.JoinType
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
        val reading = spark.sql("SELECT * FROM result")

        //去除无效数据后的条数5564902
        //        reading.filter(col("DATE") > 9999 and col("COMPANY").isNotNull and col("SOURCE") === "RESULT" and col("PROVINCE").isNotNull and col("CITY").isNotNull and col("PROVINCE").isNotNull and col("PHAID").isNull and col("PRODUCT_NAME").isNotNull and col("MOLE_NAME").isNotNull).count()

        val data = reading

        //TODO:筛选出 max_dashboard 需要用到的 keys(右边注释加*的) 即可
        //TODO:使用 COMPANY 和 MOLE_NAME 与 cpa 数据匹配，取能匹配到的，且 MKT 不为 null
        /**
          * filter
          * |-- COMPANY: string (nullable = true)       *取 isNotNull 的
          * |-- SOURCE: string (nullable = true)        *取 等于 RESULT 的
          * |-- DATE: string (nullable = true)          *取 大于 9999 的，去除只有月份或年的脏数据
          * |-- PROVINCE: string (nullable = true)      *取 isNotNull 的
          * |-- CITY: string (nullable = true)          *取 isNotNull 的
          * |-- PHAID: string (nullable = true)         取 isNull 的
          * |-- HOSP_NAME: string (nullable = true)     不用管
          * |-- CPAID: string (nullable = true)         不用管
          * |-- PRODUCT_NAME: string (nullable = true)  *取 isNotNull 的
          * |-- MOLE_NAME: string (nullable = true)     *取 isNotNull 的
          * |-- DOSAGE: string (nullable = true)        不用管
          * |-- SPEC: string (nullable = true)          不用管
          * |-- PACK_QTY: string (nullable = true)      不用管
          * |-- SALES_VALUE: string (nullable = true)   *不用管
          * |-- SALES_QTY: string (nullable = true)     不用管
          * |-- F_SALES_VALUE: string (nullable = true) 不用管
          * |-- F_SALES_QTY: string (nullable = true)   不用管
          * |-- MANUFACTURE_NAME: string (nullable = true)
          * |-- version: string (nullable = true)
          */
        val keys: List[String] = "COMPANY" :: "SOURCE" :: "DATE" :: "PROVINCE" :: "CITY" :: "PRODUCT_NAME" :: "MOLE_NAME" :: "SALES_VALUE" :: Nil
        //Check that the keys used in the aggregation are in the columns
        keys.foreach(k => {
            if (!data.columns.contains(k)) {
                logger.error(s"The key(${k}) used in the aggregation is not in the columns(${data.columns}).")
//                return data
            }
        })

        //去除脏数据，例如DATE=月份或年份的，DATE应为年月的6位数
        val formatDF = data.selectExpr(keys: _*)
            .filter(col("DATE") > 99999 and col("DATE") < 1000000 and col("COMPANY").isNotNull and col("SOURCE") === "RESULT" and col("PROVINCE").isNotNull and col("CITY").isNotNull and col("PROVINCE").isNotNull and col("PHAID").isNull and col("PRODUCT_NAME").isNotNull and col("MOLE_NAME").isNotNull)
            .withColumn("DATE", col("DATE").cast(DataTypes.IntegerType))
            .withColumn("SALES_VALUE", col("SALES_VALUE").cast(DataTypes.DoubleType))

        //缩小数据范围，需求中最小维度是分子，先计算出分子级别在单个公司年月市场、省&城市级别、产品&分子维度的聚合数据
        val moleLevelDF = formatDF.groupBy("COMPANY", "SOURCE", "DATE", "PROVINCE", "CITY", "PRODUCT_NAME", "MOLE_NAME")
            .agg(expr("SUM(SALES_VALUE) as MOLE_SALES_VALUE"))

        //TODO:用result数据与cpa数据进行匹配，得出MKT，目前cpa数据暂时写在算法里，之后匹配逻辑可能会变
        val cpa = spark.sql("SELECT * FROM cpa")
            .select("COMPANY", "PRODUCT_NAME", "MOLE_NAME", "MKT")
            .filter(col("COMPANY").isNotNull and col("PRODUCT_NAME").isNotNull and col("MOLE_NAME").isNotNull and col("MKT").isNotNull)
            .groupBy("COMPANY", "PRODUCT_NAME", "MOLE_NAME")
            .agg(first("MKT").alias("MKT"))

        //41836
        val mergeDF = moleLevelDF
            .join(cpa, moleLevelDF("COMPANY") === cpa("COMPANY") and moleLevelDF("PRODUCT_NAME") === cpa("PRODUCT_NAME") and moleLevelDF("MOLE_NAME") === cpa("MOLE_NAME"), "inner")
            .drop(cpa("COMPANY"))
            .drop(cpa("PRODUCT_NAME"))
            .drop(cpa("MOLE_NAME"))

        //TODO:因不同公司数据的数据时间维度不一样，所以分别要对每个公司的数据进行计算最新一年的数据
        val companyList = mergeDF.select("COMPANY").distinct().collect().map(_ (0)).toList.asInstanceOf[List[String]]

        val newData = companyList.map(company => {
            val companyDF = mergeDF.filter(col("COMPANY") === company)
            //TODO:得保证数据源中包含两年的数据
            val current2YearYmList = companyDF.select("DATE").distinct().sort("DATE").collect().map(_ (0)).toList.takeRight(24).asInstanceOf[List[Int]]
            val currentYearYmList = current2YearYmList.takeRight(12)

            val current2YearDF = companyDF
                .filter(col("DATE") >= current2YearYmList.min.toString.toInt && col("DATE") <= current2YearYmList.max.toString.toInt)
            val currentYearDF = computeMaxDashboardData(current2YearDF)
                .filter(col("DATE") >= currentYearYmList.min.toString.toInt && col("DATE") <= currentYearYmList.max.toString.toInt)
                .cache()
            println(company, currentYearDF.count())
            currentYearDF
        }).reduce((x, y) => x union y)

        newData.printSchema()

//        println(newData.select("MKT").distinct().collect().map(_ (0)).toList)
//        println(newData.count())
        //        println(newData.select("PRODUCT_NAME").distinct().collect().map(_(0)).toList.size)
        //        println(newData.filter(col("COMPANY") === "Pfizer").select("PRODUCT_NAME").distinct().collect().map(_(0)).toList.size)

    }

    def computeMaxDashboardData(df: DataFrame): DataFrame = {
        val moleWindow = Window
            .partitionBy("MKT", "PROVINCE", "CITY", "PRODUCT_NAME", "MOLE_NAME")

        val prodWindow = Window
            .partitionBy("MKT", "PROVINCE", "CITY", "PRODUCT_NAME")

        val cityWindow = Window
            .partitionBy("MKT", "PROVINCE", "CITY")

        val provWindow = Window
            .partitionBy("MKT", "PROVINCE")

        val mktWindow = Window
            .partitionBy("MKT")

        //TODO:产品在不同范围内的销售额和份额都是不同的，PROD_IN_CITY,PROD_IN_PROV,PROD_IN_MKT,PROD_IN_COMPANY
        //TODO:数据是否正确需要核对
        df.withColumn("PROD_SALES_VALUE", sum("MOLE_SALES_VALUE").over(currMonthWindow(prodWindow)))
            .withColumn("PROD_SALES_IN_COMPANY_VALUE", sum("MOLE_SALES_VALUE").over(currMonthWindow(Window.partitionBy("PRODUCT_NAME"))))
            .withColumn("PROD_SALES_IN_COMPANY_RANK", dense_rank.over(Window.partitionBy("DATE").orderBy(col("PROD_SALES_IN_COMPANY_VALUE").desc)))
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
        window.orderBy(col("DATE").cast(DataTypes.IntegerType)).rangeBetween(0, 0)
    }

    def lastMonthWindow(window: WindowSpec): WindowSpec = {
        window
            .orderBy(to_date(col("DATE").cast("string"), "yyyyMM").cast("timestamp").cast("long"))
            .rangeBetween(-86400 * 31, -86400 * 28)
    }


    def lastYearWindow(window: WindowSpec): WindowSpec =
        window.orderBy(col("DATE").cast(DataTypes.IntegerType)).rangeBetween(-100, -100)

}
