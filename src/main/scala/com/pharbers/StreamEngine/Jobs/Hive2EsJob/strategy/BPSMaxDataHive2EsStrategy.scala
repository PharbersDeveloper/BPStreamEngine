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

        //TODO:用result数据与cpa数据进行匹配，得出MKT，目前cpa数据 暂时 写在算法里，之后匹配逻辑可能会变
        val cpa = spark.sql("SELECT * FROM cpa")
            .select("COMPANY", "PRODUCT_NAME", "MOLE_NAME", "MKT")
            .filter(col("COMPANY").isNotNull and col("PRODUCT_NAME").isNotNull and col("MOLE_NAME").isNotNull and col("MKT").isNotNull)
            .groupBy("COMPANY", "PRODUCT_NAME", "MOLE_NAME")
            .agg(first("MKT").alias("MKT"))

        //20200114-结果数据总count-41836
        val mergeDF = moleLevelDF
            .join(cpa, moleLevelDF("COMPANY") === cpa("COMPANY") and moleLevelDF("PRODUCT_NAME") === cpa("PRODUCT_NAME") and moleLevelDF("MOLE_NAME") === cpa("MOLE_NAME"), "inner")
            .drop(cpa("COMPANY"))
            .drop(cpa("PRODUCT_NAME"))
            .drop(cpa("MOLE_NAME"))

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
            .GenerateSalesRowWith("CURR", "MOLE", "CITY")
            .GenerateSalesRowWith("CURR", "MOLE", "PROV")
            .GenerateSalesRowWith("CURR", "MOLE", "NATION")
            .GenerateSalesRowWith("CURR", "PROD", "CITY")
            .GenerateSalesRowWith("CURR", "PROD", "PROV")
            .GenerateSalesRowWith("CURR", "PROD", "NATION")
            .GenerateSalesRowWith("LAST_M", "MOLE", "CITY")
            .GenerateSalesRowWith("LAST_M", "MOLE", "PROV")
            .GenerateSalesRowWith("LAST_M", "MOLE", "NATION")
            .GenerateSalesRowWith("LAST_M", "PROD", "CITY")
            .GenerateSalesRowWith("LAST_M", "PROD", "PROV")
            .GenerateSalesRowWith("LAST_M", "PROD", "NATION")
            .GenerateSalesRowWith("LAST_Y", "MOLE", "CITY")
            .GenerateSalesRowWith("LAST_Y", "MOLE", "PROV")
            .GenerateSalesRowWith("LAST_Y", "MOLE", "NATION")
            .GenerateSalesRowWith("LAST_Y", "PROD", "CITY")
            .GenerateSalesRowWith("LAST_Y", "PROD", "PROV")
            .GenerateSalesRowWith("LAST_Y", "PROD", "NATION")
            .GenerateRankAndShareRowWith("CURR","MOLE","CITY", "PROV")
            .GenerateRankAndShareRowWith("CURR","MOLE","CITY", "NATION")
            .GenerateRankAndShareRowWith("CURR","MOLE","PROV", "NATION")
            .GenerateRankAndShareRowWith("CURR","PROD","CITY", "PROV")
            .GenerateRankAndShareRowWith("CURR","PROD","CITY", "NATION")
            .GenerateRankAndShareRowWith("CURR","PROD","PROV", "NATION")
            .GenerateRankAndShareRowWith("LAST_M","MOLE","CITY", "PROV")
            .GenerateRankAndShareRowWith("LAST_M","MOLE","CITY", "NATION")
            .GenerateRankAndShareRowWith("LAST_M","MOLE","PROV", "NATION")
            .GenerateRankAndShareRowWith("LAST_M","PROD","CITY", "PROV")
            .GenerateRankAndShareRowWith("LAST_M","PROD","CITY", "NATION")
            .GenerateRankAndShareRowWith("LAST_M","PROD","PROV", "NATION")
            .GenerateRankAndShareRowWith("LAST_Y","MOLE","CITY", "PROV")
            .GenerateRankAndShareRowWith("LAST_Y","MOLE","CITY", "NATION")
            .GenerateRankAndShareRowWith("LAST_Y","MOLE","PROV", "NATION")
            .GenerateRankAndShareRowWith("LAST_Y","PROD","CITY", "PROV")
            .GenerateRankAndShareRowWith("LAST_Y","PROD","CITY", "NATION")
            .GenerateRankAndShareRowWith("LAST_Y","PROD","PROV", "NATION")
            .GenerateGrowthRowWith("MOM", "MOLE", "CITY")
            .GenerateGrowthRowWith("MOM", "MOLE", "PROV")
            .GenerateGrowthRowWith("MOM", "MOLE", "NATION")
            .GenerateGrowthRowWith("MOM", "PROD", "CITY")
            .GenerateGrowthRowWith("MOM", "PROD", "PROV")
            .GenerateGrowthRowWith("MOM", "PROD", "NATION")
            .GenerateGrowthRowWith("YOY", "MOLE", "CITY")
            .GenerateGrowthRowWith("YOY", "MOLE", "PROV")
            .GenerateGrowthRowWith("YOY", "MOLE", "NATION")
            .GenerateGrowthRowWith("YOY", "PROD", "CITY")
            .GenerateGrowthRowWith("YOY", "PROD", "PROV")
            .GenerateGrowthRowWith("YOY", "PROD", "NATION")
            .GenerateEIRowWith("MOLE", "CITY", "PROV")
            .GenerateEIRowWith("MOLE", "CITY", "NATION")
            .GenerateEIRowWith("MOLE", "PROV", "NATION")
            .GenerateEIRowWith("PROD", "CITY", "PROV")
            .GenerateEIRowWith("PROD", "CITY", "NATION")
            .GenerateEIRowWith("PROD", "PROV", "NATION")
            .df

    }

}
