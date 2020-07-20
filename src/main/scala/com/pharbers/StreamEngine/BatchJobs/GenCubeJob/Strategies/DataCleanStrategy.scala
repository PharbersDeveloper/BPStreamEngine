package com.pharbers.StreamEngine.BatchJobs.GenCubeJob.Strategies

import com.pharbers.StreamEngine.Utils.Log.PhLogable
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataCleanStrategy(spark: SparkSession) extends PhLogable {

    //补齐所需列 QUARTER COUNTRY MKT
    def clean(df: DataFrame): DataFrame = {

        val keys: List[String] = "COMPANY" :: "SOURCE" :: "DATE" :: "PROVINCE" :: "CITY" :: "PRODUCT_NAME" :: "MOLE_NAME" :: "SALES_VALUE" :: "SALES_QTY" :: Nil
        //Check that the keys used in the aggregation are in the columns
        keys.foreach(k => {
            if (!df.columns.contains(k)) {
                logger.error(s"The key(${k}) used in the aggregation is not in the columns(${df.columns}).")
                //                return data
            }
        })

        //去除脏数据，例如DATE=月份或年份的，DATE应为年月的6位数
        val formatDF = df.selectExpr(keys: _*)
                .filter(col("DATE") > 99999 and col("DATE") < 1000000 and col("COMPANY").isNotNull and col("SOURCE") === "RESULT" and col("PROVINCE").isNotNull and col("CITY").isNotNull and col("PROVINCE").isNotNull and col("PHAID").isNull and col("PRODUCT_NAME").isNotNull and col("MOLE_NAME").isNotNull)
                .withColumn("SALES_VALUE", col("SALES_VALUE").cast(DataTypes.DoubleType))
                .withColumn("SALES_QTY", col("SALES_QTY").cast(DataTypes.DoubleType))

        //缩小数据范围，需求中最小维度是分子，先计算出分子级别在单个公司年月市场、省&城市级别、产品&分子维度的聚合数据
        //补齐所需列 QUARTER COUNTRY MKT
        //删除不需列 DATE
        val moleLevelDF = formatDF.groupBy("COMPANY", "DATE", "PROVINCE", "CITY", "PRODUCT_NAME", "MOLE_NAME")
                .agg(expr("SUM(SALES_VALUE) as SALES_VALUE"), expr("SUM(SALES_QTY) as SALES_QTY"))
                .withColumn("YEAR", col("DATE").substr(0, 4).cast(DataTypes.IntegerType))
                .withColumn("DATE", col("DATE").cast(DataTypes.IntegerType))
                .withColumn("MONTH", col("DATE") - col("YEAR") * 100)
                .withColumn("QUARTER", ((col("MONTH") - 1) / 3) + 1)
                .withColumn("QUARTER", col("QUARTER").cast(DataTypes.IntegerType))
                .withColumn("COUNTRY", lit("CHINA"))
                .withColumn("APEX", lit("PHARBERS"))
                .drop("DATE")

        //TODO:临时处理信立泰
        val moleLevelDF1 = moleLevelDF.filter(col("COMPANY") === "信立泰")
        val moleLevelDF2 = moleLevelDF.filter(col("COMPANY") =!= "信立泰")

        //TODO:用result数据与cpa数据进行匹配，得出MKT，目前cpa数据 暂时 写在算法里，之后匹配逻辑可能会变
        val cpa = spark.sql("SELECT * FROM cpa")
                .select("COMPANY", "PRODUCT_NAME", "MOLE_NAME", "MKT")
                .filter(col("COMPANY").isNotNull and col("PRODUCT_NAME").isNotNull and col("MOLE_NAME").isNotNull and col("MKT").isNotNull)
                .groupBy("COMPANY", "PRODUCT_NAME", "MOLE_NAME")
                .agg(first("MKT").alias("MKT"))

        //TODO:临时处理信立泰
        val mergeDF1 = moleLevelDF1.withColumn("MKT", lit("抗血小板市场"))
        val mergeDF2 = moleLevelDF2
                .join(cpa, moleLevelDF2("COMPANY") === cpa("COMPANY") and moleLevelDF2("PRODUCT_NAME") === cpa("PRODUCT_NAME") and moleLevelDF2("MOLE_NAME") === cpa("MOLE_NAME"), "inner")
                .drop(cpa("COMPANY"))
                .drop(cpa("PRODUCT_NAME"))
                .drop(cpa("MOLE_NAME"))

        mergeDF1 union mergeDF2

    }

}
