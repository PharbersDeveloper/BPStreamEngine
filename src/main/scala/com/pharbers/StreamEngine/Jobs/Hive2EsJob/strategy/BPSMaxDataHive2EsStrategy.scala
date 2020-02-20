package com.pharbers.StreamEngine.Jobs.Hive2EsJob.strategy

import com.pharbers.util.log.PhLogable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

case class BPSMaxDataHive2EsStrategy(spark: SparkSession) extends BPSStrategy[DataFrame] with PhLogable {

    override def convert(data: DataFrame): DataFrame = {
        //筛选出 max_dashboard 需要用到的 keys(右边注释加*的) 即可
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

        //TODO:临时处理信立泰
        val moleLevelDF1 = moleLevelDF.filter(col("COMPANY") === "信立泰")
        val moleLevelDF2 = moleLevelDF.filter(col("COMPANY") =!= "信立泰")

        //TODO:用result数据与cpa数据进行匹配，得出MKT，目前cpa数据 暂时 写在算法里，之后匹配逻辑可能会变
        val cpa = spark.sql("SELECT * FROM cpa")
            .select("COMPANY", "PRODUCT_NAME", "MOLE_NAME", "MKT")
            .filter(col("COMPANY").isNotNull and col("PRODUCT_NAME").isNotNull and col("MOLE_NAME").isNotNull and col("MKT").isNotNull)
            .groupBy("COMPANY", "PRODUCT_NAME", "MOLE_NAME")
            .agg(first("MKT").alias("MKT"))

//        //20200114-结果数据总count-41836
//        val mergeDF = moleLevelDF
//            .join(cpa, moleLevelDF("COMPANY") === cpa("COMPANY") and moleLevelDF("PRODUCT_NAME") === cpa("PRODUCT_NAME") and moleLevelDF("MOLE_NAME") === cpa("MOLE_NAME"), "inner")
//            .drop(cpa("COMPANY"))
//            .drop(cpa("PRODUCT_NAME"))
//            .drop(cpa("MOLE_NAME"))

        //TODO:临时处理信立泰
        val mergeDF1 = moleLevelDF1.withColumn("MKT", lit("抗血小板市场"))
        val mergeDF2 = moleLevelDF2
            .join(cpa, moleLevelDF2("COMPANY") === cpa("COMPANY") and moleLevelDF2("PRODUCT_NAME") === cpa("PRODUCT_NAME") and moleLevelDF2("MOLE_NAME") === cpa("MOLE_NAME"), "inner")
            .drop(cpa("COMPANY"))
            .drop(cpa("PRODUCT_NAME"))
            .drop(cpa("MOLE_NAME"))
        val mergeDF = mergeDF1 union mergeDF2

        //TODO:因不同公司数据的数据时间维度不一样，所以分别要对每个公司的数据进行计算最新一年的数据
        val companyList = mergeDF.select("COMPANY").distinct().collect().map(_ (0)).toList.asInstanceOf[List[String]]

        //20200114-分公司计算最新一年的count-21437
        companyList.map(company => {
            val companyDF = mergeDF.filter(col("COMPANY") === company)
            //TODO:得保证数据源中包含两年的数据
            val current2YearYmList = companyDF.select("DATE").distinct().sort("DATE").collect().map(_ (0)).toList.takeRight(24).asInstanceOf[List[Int]]
            val currentYearYmList = current2YearYmList.takeRight(12)

            val current2YearDF = companyDF
                .filter(col("DATE") >= current2YearYmList.min.toString.toInt && col("DATE") <= current2YearYmList.max.toString.toInt)
            val currentYearDF = computeMaxDashboardData(current2YearDF)
                .filter(col("DATE") >= currentYearYmList.min.toString.toInt && col("DATE") <= currentYearYmList.max.toString.toInt)
                .cache()
            currentYearDF
        }).reduce((x, y) => x union y).na.fill(0.0)

    }

    def computeMaxDashboardData(df: DataFrame): DataFrame = {

        //TODO:产品在不同范围内的销售额和份额都是不同的，PROD_IN_CITY,PROD_IN_PROV,PROD_IN_MKT,PROD_IN_COMPANY
        //TODO:数据是否正确需要核对
        PhMaxDashboardWindowFunc(df)
            .GenerateSalesAndRankRowWith("CURR", "MOLE", "CITY")
            .GenerateSalesAndRankRowWith("CURR", "MOLE", "PROV")
            .GenerateSalesAndRankRowWith("CURR", "MOLE", "NATION")
            .GenerateSalesAndRankRowWith("CURR", "PROD", "CITY")
            .GenerateSalesAndRankRowWith("CURR", "PROD", "PROV")
            .GenerateSalesAndRankRowWith("CURR", "PROD", "NATION")
            .GenerateSalesAndRankRowWith("CURR", "TOTAL", "CITY")
            .GenerateSalesAndRankRowWith("CURR", "TOTAL", "PROV")
            .GenerateSalesAndRankRowWith("CURR", "TOTAL", "NATION")
            .GenerateSalesAndRankRowWith("CURR", "MKT", "CITY")
            .GenerateSalesAndRankRowWith("CURR", "MKT", "PROV")
            .GenerateSalesAndRankRowWith("CURR", "MKT", "NATION")
            .GenerateSalesAndRankRowWith("LAST_M", "MOLE", "CITY")
            .GenerateSalesAndRankRowWith("LAST_M", "MOLE", "PROV")
            .GenerateSalesAndRankRowWith("LAST_M", "MOLE", "NATION")
            .GenerateSalesAndRankRowWith("LAST_M", "PROD", "CITY")
            .GenerateSalesAndRankRowWith("LAST_M", "PROD", "PROV")
            .GenerateSalesAndRankRowWith("LAST_M", "PROD", "NATION")
            .GenerateSalesAndRankRowWith("LAST_M", "TOTAL", "CITY")
            .GenerateSalesAndRankRowWith("LAST_M", "TOTAL", "PROV")
            .GenerateSalesAndRankRowWith("LAST_M", "TOTAL", "NATION")
            .GenerateSalesAndRankRowWith("LAST_M", "MKT", "CITY")
            .GenerateSalesAndRankRowWith("LAST_M", "MKT", "PROV")
            .GenerateSalesAndRankRowWith("LAST_M", "MKT", "NATION")
            .GenerateSalesAndRankRowWith("LAST_Y", "MOLE", "CITY")
            .GenerateSalesAndRankRowWith("LAST_Y", "MOLE", "PROV")
            .GenerateSalesAndRankRowWith("LAST_Y", "MOLE", "NATION")
            .GenerateSalesAndRankRowWith("LAST_Y", "PROD", "CITY")
            .GenerateSalesAndRankRowWith("LAST_Y", "PROD", "PROV")
            .GenerateSalesAndRankRowWith("LAST_Y", "PROD", "NATION")
            .GenerateSalesAndRankRowWith("LAST_Y", "TOTAL", "CITY")
            .GenerateSalesAndRankRowWith("LAST_Y", "TOTAL", "PROV")
            .GenerateSalesAndRankRowWith("LAST_Y", "TOTAL", "NATION")
            .GenerateSalesAndRankRowWith("LAST_Y", "MKT", "CITY")
            .GenerateSalesAndRankRowWith("LAST_Y", "MKT", "PROV")
            .GenerateSalesAndRankRowWith("LAST_Y", "MKT", "NATION")
            .GenerateShareRowWith("CURR","MOLE","CITY")
            .GenerateShareRowWith("CURR","MOLE","PROV")
            .GenerateShareRowWith("CURR","MOLE","NATION")
            .GenerateShareRowWith("CURR","PROD","CITY")
            .GenerateShareRowWith("CURR","PROD","PROV")
            .GenerateShareRowWith("CURR","PROD","NATION")
            .GenerateShareRowWith("CURR","MKT","CITY")
            .GenerateShareRowWith("CURR","MKT","PROV")
            .GenerateShareRowWith("CURR","MKT","NATION")
            .GenerateShareRowWith("LAST_M","MOLE","CITY")
            .GenerateShareRowWith("LAST_M","MOLE","PROV")
            .GenerateShareRowWith("LAST_M","MOLE","NATION")
            .GenerateShareRowWith("LAST_M","PROD","CITY")
            .GenerateShareRowWith("LAST_M","PROD","PROV")
            .GenerateShareRowWith("LAST_M","PROD","NATION")
            .GenerateShareRowWith("LAST_M","MKT","CITY")
            .GenerateShareRowWith("LAST_M","MKT","PROV")
            .GenerateShareRowWith("LAST_M","MKT","NATION")
            .GenerateShareRowWith("LAST_Y","MOLE","CITY")
            .GenerateShareRowWith("LAST_Y","MOLE","PROV")
            .GenerateShareRowWith("LAST_Y","MOLE","NATION")
            .GenerateShareRowWith("LAST_Y","PROD","CITY")
            .GenerateShareRowWith("LAST_Y","PROD","PROV")
            .GenerateShareRowWith("LAST_Y","PROD","NATION")
            .GenerateShareRowWith("LAST_Y","MKT","CITY")
            .GenerateShareRowWith("LAST_Y","MKT","PROV")
            .GenerateShareRowWith("LAST_Y","MKT","NATION")
            .GenerateGrowthRowWith("MOM", "MOLE", "CITY")
            .GenerateGrowthRowWith("MOM", "MOLE", "PROV")
            .GenerateGrowthRowWith("MOM", "MOLE", "NATION")
            .GenerateGrowthRowWith("MOM", "PROD", "CITY")
            .GenerateGrowthRowWith("MOM", "PROD", "PROV")
            .GenerateGrowthRowWith("MOM", "PROD", "NATION")
            .GenerateGrowthRowWith("MOM", "MKT", "CITY")
            .GenerateGrowthRowWith("MOM", "MKT", "PROV")
            .GenerateGrowthRowWith("MOM", "MKT", "NATION")
            .GenerateGrowthRowWith("YOY", "MOLE", "CITY")
            .GenerateGrowthRowWith("YOY", "MOLE", "PROV")
            .GenerateGrowthRowWith("YOY", "MOLE", "NATION")
            .GenerateGrowthRowWith("YOY", "PROD", "CITY")
            .GenerateGrowthRowWith("YOY", "PROD", "PROV")
            .GenerateGrowthRowWith("YOY", "PROD", "NATION")
            .GenerateGrowthRowWith("YOY", "MKT", "CITY")
            .GenerateGrowthRowWith("YOY", "MKT", "PROV")
            .GenerateGrowthRowWith("YOY", "MKT", "NATION")
            .GenerateEIRowWith("MOLE", "CITY")
            .GenerateEIRowWith("MOLE", "PROV")
            .GenerateEIRowWith("MOLE", "NATION")
            .GenerateEIRowWith("PROD", "CITY")
            .GenerateEIRowWith("PROD", "PROV")
            .GenerateEIRowWith("PROD", "NATION")
            .GenerateEIRowWith("MKT", "CITY")
            .GenerateEIRowWith("MKT", "PROV")
            .GenerateEIRowWith("MKT", "NATION")
            .df

    }

}
