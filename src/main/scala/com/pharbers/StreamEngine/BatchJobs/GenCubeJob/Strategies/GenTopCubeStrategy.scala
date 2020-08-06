package com.pharbers.StreamEngine.BatchJobs.GenCubeJob.Strategies

import com.pharbers.StreamEngine.Utils.Log.PhLogable
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class GenTopCubeStrategy(spark: SparkSession) extends GenCubeStrategyTrait with PhLogable {



    def genTopCube(listCube: List[DataFrame], top: Int): List[DataFrame] = {

        logger.info("Start exec gen-top-cube strategy.")

        listCube.map( cube => {
            val firstRow = cube.first()
            val name = firstRow.getAs[String]("DIMENSION_NAME")
            val value = firstRow.getAs[String]("DIMENSION_VALUE")
            logger.info(s"=======> name(${name}) value(${value})")
            if (name == "3-time-geo-prod") {
                val arr = value.split("-")
                if (arr.size != 3) logger.error("DIMENSION_VALUE get error!")
                val time = arr(0)
                val geo = arr(1)
                val prod = arr(2)

                val geoFaGroup = getFatherHierarchiesByDiKey("geo", geo, Map("geo" -> List("COUNTRY", "PROVINCE", "CITY")))
                val prodFaGroup = getFatherHierarchiesByDiKey("prod", prod, Map("prod" -> List("COMPANY", "MKT", "MOLE_NAME", "PRODUCT_NAME")))
                val prodRankWindow: WindowSpec = currTimeWindow(time, Window.partitionBy("DIMENSION_VALUE", (geoFaGroup :+ geo) ::: prodFaGroup:_*)).orderBy(col("SALES_VALUE").desc)
                if (prod == "PRODUCT_NAME") {
                    val mktWindow: WindowSpec = Window.partitionBy("DIMENSION_VALUE", (geoFaGroup :+ geo) ::: List("COMPANY", "MKT"): _*)
                    get3DTopDF(addProdInMktColumn(cube, mktWindow, time), prodRankWindow, top)
                } else {
                    get3DTopDF(cube, prodRankWindow, top)
                }

            } else  getTopDF(cube, top)

        })

    }

    //跨层级(mkt-mole-product)的唯一需求: 产品在市场上的表现
    private def addProdInMktColumn(data: DataFrame, ws: WindowSpec, time: String): DataFrame = {
        data
            //市场规模
            .withColumn("MKT_SALES_VALUE", sum("SALES_VALUE").over(currTimeWindow(time, ws)))
            //上期市场规模
            .withColumn("LAST_MKT_SALES_VALUE", sum("SALES_VALUE").over(lastTimeWindow(time, ws)))
            //市场增长
            .withColumn("MKT_SALES_VALUE_GROWTH", when(col("LAST_MKT_SALES_VALUE") === 0.0, 0.0).otherwise(col("MKT_SALES_VALUE") - col("LAST_MKT_SALES_VALUE")))
            //市场增长率
            .withColumn("MKT_SALES_VALUE_GROWTH_RATE", when(col("MKT_SALES_VALUE_GROWTH") === 0.0, 0.0).otherwise(col("MKT_SALES_VALUE_GROWTH") / col("LAST_MKT_SALES_VALUE")))
            //产品市场份额
            .withColumn("PROD_IN_MKT_SALES_VALUE_SHARE", when(col("MKT_SALES_VALUE") === 0.0, 0.0).otherwise(col("SALES_VALUE") / col("MKT_SALES_VALUE")))
            //上期产品市场份额
            .withColumn("LAST_PROD_IN_MKT_SALES_VALUE_SHARE", when(col("LAST_MKT_SALES_VALUE") === 0.0, 0.0).otherwise(col("LAST_SALES_VALUE") / col("LAST_MKT_SALES_VALUE")))
            //产品市场EI
            .withColumn("PROD_IN_MKT_SALES_VALUE_EI", when(col("LAST_PROD_IN_MKT_SALES_VALUE_SHARE") === 0.0, 0.0).otherwise(col("PROD_IN_MKT_SALES_VALUE_SHARE") / col("LAST_PROD_IN_MKT_SALES_VALUE_SHARE") * 100))
    }

    //common
    private def getTopDF(data: DataFrame, top: Int): DataFrame = {
        data.sort(desc("SALES_VALUE")).limit(top)
    }

    //3维度(3-time-geo-prod)时，定2变1，定time和geo，取top prod
    private def get3DTopDF(data: DataFrame, ws: WindowSpec, top: Int): DataFrame = {
        data.withColumn("PROD_RANK", row_number().over(ws)).filter(col("PROD_RANK") <= top).drop("PROD_RANK")
    }

}
