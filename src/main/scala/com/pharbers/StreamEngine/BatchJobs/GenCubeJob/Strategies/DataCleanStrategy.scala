package com.pharbers.StreamEngine.BatchJobs.GenCubeJob.Strategies

import com.pharbers.StreamEngine.Utils.Log.PhLogable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataCleanStrategy(spark: SparkSession) extends PhLogable {

    //补齐所需列 QUARTER COUNTRY MKT
    def clean(df: DataFrame): DataFrame = {

        /**
          * |-- PHA: string (nullable = true)
            |-- Province: string (nullable = true)
            |-- City: string (nullable = true)
            |-- Date: double (nullable = true)
            |-- Molecule: string (nullable = true)
            |-- Prod_Name: string (nullable = true)
            |-- BEDSIZE: double (nullable = true)
            |-- PANEL: double (nullable = true)
            |-- Seg: double (nullable = true)
            |-- Predict_Sales: double (nullable = true)
            |-- Predict_Unit: double (nullable = true)
            |-- version: string (nullable = true)
            |-- company: string (nullable = true)
          */

        val renamedDF = df
            .withColumnRenamed("PHA", "PHAID")
            .withColumnRenamed("Province", "PROVINCE")
            .withColumnRenamed("City", "CITY")
            .withColumnRenamed("Date", "DATE")
            .withColumnRenamed("Molecule", "MOLE_NAME")
            .withColumnRenamed("Prod_Name", "PRODUCT_NAME")
            .withColumnRenamed("Predict_Sales", "SALES_VALUE")
            .withColumnRenamed("Predict_Unit", "SALES_QTY")
            .withColumnRenamed("company", "COMPANY")

        val keys: List[String] = "COMPANY" :: "DATE" :: "PROVINCE" :: "CITY" :: "PRODUCT_NAME" :: "MOLE_NAME" :: "SALES_VALUE" :: "SALES_QTY" :: Nil
        //Check that the keys used in the aggregation are in the columns
        keys.foreach(k => {
            if (!renamedDF.columns.contains(k)) {
                logger.error(s"The key(${k}) used in the aggregation is not in the columns(${renamedDF.columns}).")
                //                return data
            }
        })

        //去除脏数据，例如DATE=月份或年份的，DATE应为年月的6位数 renamedDF.filter(col("DATE") > 99999 and col("DATE") < 1000000 and col("COMPANY").isNotNull).count()
        val formatDF = renamedDF
                .selectExpr(keys: _*)
                .filter(col("DATE") > 99999 and col("DATE") < 1000000 and col("COMPANY").isNotNull and col("PROVINCE").isNotNull and col("CITY").isNotNull  and col("PHAID").isNotNull and col("PRODUCT_NAME").isNotNull and col("MOLE_NAME").isNotNull)
                .withColumn("SALES_VALUE", col("SALES_VALUE").cast(DataTypes.DoubleType))
                .withColumn("SALES_QTY", col("SALES_QTY").cast(DataTypes.DoubleType))

        //缩小数据范围，需求中最小维度是分子，先计算出分子级别在单个公司年月市场、省&城市级别、产品&分子维度的聚合数据
        //补齐所需列 QUARTER COUNTRY MKT
        val moleLevelDF = formatDF.groupBy("COMPANY", "DATE", "PROVINCE", "CITY", "PRODUCT_NAME", "MOLE_NAME")
                .agg(expr("SUM(SALES_VALUE) as SALES_VALUE"), expr("SUM(SALES_QTY) as SALES_QTY"))
                .withColumn("YEAR", col("DATE").substr(0, 4).cast(DataTypes.IntegerType))
                .withColumn("DATE", col("DATE").cast(DataTypes.IntegerType))
                .withColumn("MONTH", col("DATE") - col("YEAR") * 100)
                .withColumn("QUARTER", ((col("MONTH") - 1) / 3) + 1)
                .withColumn("QUARTER", col("QUARTER").cast(DataTypes.IntegerType))
                .withColumn("COUNTRY", lit("CHINA"))
                .withColumn("APEX", lit("PHARBERS"))

//        //TODO:临时处理信立泰
//        val moleLevelDF1 = moleLevelDF.filter(col("COMPANY") === "信立泰")
//        val moleLevelDF2 = moleLevelDF.filter(col("COMPANY") =!= "信立泰")
//
//        //TODO:用result数据与cpa数据进行匹配，得出MKT，目前cpa数据 暂时 写在算法里，之后匹配逻辑可能会变
//        val cpa = spark.sql("SELECT * FROM cpa")
//                .select("COMPANY", "PRODUCT_NAME", "MOLE_NAME", "MKT")
//                .filter(col("COMPANY").isNotNull and col("PRODUCT_NAME").isNotNull and col("MOLE_NAME").isNotNull and col("MKT").isNotNull)
//                .groupBy("COMPANY", "PRODUCT_NAME", "MOLE_NAME")
//                .agg(first("MKT").alias("MKT"))

//        //TODO:临时处理信立泰
//        val mergeDF1 = moleLevelDF1.withColumn("MKT", lit("抗血小板市场"))
//        val mergeDF2 = moleLevelDF2
//                .join(cpa, moleLevelDF2("COMPANY") === cpa("COMPANY") and moleLevelDF2("PRODUCT_NAME") === cpa("PRODUCT_NAME") and moleLevelDF2("MOLE_NAME") === cpa("MOLE_NAME"), "inner")
//                .drop(cpa("COMPANY"))
//                .drop(cpa("PRODUCT_NAME"))
//                .drop(cpa("MOLE_NAME"))
//
//        val mergeDF = mergeDF1 union mergeDF2

        //TODO:因不同公司数据的数据时间维度不一样，所以分别要对每个公司的数据进行计算最新一年的数据
        val companyList = moleLevelDF.select("COMPANY").distinct().collect().map(_ (0)).toList.asInstanceOf[List[String]]

        companyList.map(company => {
            val companyDF = moleLevelDF.filter(col("COMPANY") === company)
            //TODO:得保证数据源中包含两年的数据
            val current2YearYmList = companyDF.select("DATE").distinct().sort("DATE").collect().map(_ (0)).toList.takeRight(24).asInstanceOf[List[Int]]
            companyDF
                .filter(col("DATE") >= current2YearYmList.min.toString.toInt && col("DATE") <= current2YearYmList.max.toString.toInt)
        }).reduce((x, y) => x union y).na.fill(0.0)

    }

}
