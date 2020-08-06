package com.pharbers.StreamEngine.BatchJobs.GenCubeJob.Strategies

import com.pharbers.StreamEngine.Utils.Log.PhLogable
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

class GenCubeStrategy(spark: SparkSession) extends GenCubeStrategyTrait with PhLogable {

    var dimensions: Map[String, List[String]] = Map.empty
    var measures: List[String] = List.empty
    var allHierarchies: List[String] = List.empty
    var cuboids: List[Map[String, List[String]]] = List.empty
    var unifiedColumns: Array[String] = Array.empty

    def convert(data: DataFrame): List[DataFrame] = {

        logger.info("Start exec gen-cube strategy.")

        dimensions = initDimensions()
        measures = initMeasures()
        allHierarchies = "APEX" :: initAllHierarchies()
        cuboids = initCuboids()

        val cuboidsData = genCuboidsData(data)

        //加一个函数只对所需维度组合求计算度量
        genMaxComputedCube(cuboidsData)

    }

    //TODO:关于Dimensions和Measures的定义，是放在文件(local/hdfs)里定义，还是在启动job时以参数传递？待优化
    private def initDimensions(): Map[String, List[String]] = {
        Map(
            "time" -> List("YEAR", "QUARTER", "MONTH"), // "YEAR", "QUARTER", "MONTH" 是 result 原数据中没有的, 由DATE(YM)转变
            "geo" -> List("COUNTRY", "PROVINCE", "CITY"), // COUNTRY 是 result 原数据中没有的
            "prod" -> List("COMPANY", "MKT", "MOLE_NAME", "PRODUCT_NAME") // MKT 是 result 原数据中没有的
        )
    }

    private def initMeasures(): List[String] = List("SALES_VALUE", "SALES_QTY")

    //TODO:check dimensions and measures 是否在 数据源中

    private def initCuboids(): List[Map[String, List[String]]] = {
        var cuboids: List[Map[String, List[String]]] = List.empty
        for (s <- dimensions.keySet.subsets()) {
            var cuboid: Map[String, List[String]] = Map.empty
            for (k <- s) {
                cuboid += (k -> dimensions(k))
            }
            cuboids = cuboids :+ cuboid
        }
        cuboids
    }

    private def initAllHierarchies(): List[String] = {
        dimensions.values.reduce((x, y) => x ::: y)
    }

    //对每个维度的排列组合进行聚合
    private def genCuboidsData(df: DataFrame): List[DataFrame] = {

        var listDF: List[DataFrame] = List.empty

        for (cuboid <- cuboids) {
            listDF = listDF ::: genCuboidData(df, cuboid)
        }

        listDF

    }

    private def genCuboidData(df: DataFrame, cuboid: Map[String, List[String]]): List[DataFrame] = {

        cuboid.size match {
            case 0 => genApexCube(df) :: Nil
            //            case x if x == dimensions.size => genMultiDimensionsCube(df, cuboid)
            case _ => genMultiDimensionsCube(df, cuboid)
        }

    }

    private def genApexCube(df: DataFrame): DataFrame = {

        val apexDF = df.groupBy("APEX").sum(measures: _*)
            .drop(measures: _*)
            .withColumnRenamed("sum(SALES_VALUE)", "SALES_VALUE")
            .withColumnRenamed("sum(SALES_QTY)", "SALES_QTY")
            .withColumn("DIMENSION_NAME", lit("apex"))
            .withColumn("DIMENSION_VALUE", lit("*"))
            .withColumn("SALES_VALUE_RANK", dense_rank.over(Window.partitionBy("APEX").orderBy(desc("SALES_VALUE"))))  //以 SALES_VALUE 降序排序
            .withColumn("SALES_QTY_RANK", dense_rank.over(Window.partitionBy("APEX").orderBy(desc("SALES_QTY"))))  //以 SALES_QTY 降序排序

        val apexCube = fillLostKeys(apexDF)
        unifiedColumns = apexCube.columns
        apexCube

    }

    //TODO:不用baseCube了，直接求multiDimensionsCube
    private def genBaseCube(df: DataFrame): DataFrame = fillLostKeys(
        df
                .withColumn("DIMENSION_NAME", lit("base"))
                .withColumn("DIMENSION_VALUE", lit("*"))
    )

    private def genMultiDimensionsCube(df: DataFrame, cuboid: Map[String, List[String]]): List[DataFrame] = {

        var listDF: List[DataFrame] = List.empty
        val dimensionsName = getDimensionsName(cuboid)
        for (one_hierarchies_group <- genCartesianHierarchies(cuboid)) {
            val agg_group = fillFullHierarchies(one_hierarchies_group.toList, dimensions)
            val time_group = getTimeHierarchies(one_hierarchies_group.toList, dimensions)
            val tmpDF = df.groupBy(agg_group.head, agg_group.tail: _*).sum(measures: _*)
                .drop(measures: _*)
                .withColumnRenamed("sum(SALES_VALUE)", "SALES_VALUE")
                .withColumnRenamed("sum(SALES_QTY)", "SALES_QTY")
                .withColumn("DIMENSION_NAME", lit(dimensionsName))
                .withColumn("DIMENSION_VALUE", lit(one_hierarchies_group.mkString("-")))
                //TODO: 3维度时，需要定两个变量来排名
                .withColumn("SALES_VALUE_RANK", dense_rank.over(Window.partitionBy("DIMENSION_VALUE", time_group: _*).orderBy(desc("SALES_VALUE")))) //以时间维度分partition才有排名的意义，受限于时间维度上年/季/月层次
                .withColumn("SALES_QTY_RANK", dense_rank.over(Window.partitionBy("DIMENSION_VALUE", time_group: _*).orderBy(desc("SALES_QTY")))) //以时间维度分partition才有排名的意义，受限于时间维度上年/季/月层次
            listDF = listDF :+ fillLostKeys(tmpDF)
        }
        listDF

    }

    private def getDimensionsName(cuboid: Map[String, List[String]]): String = {
        if (cuboid.isEmpty) return "*"
        cuboid.size + "-" + cuboid.keySet.mkString("-")
    }

    private def fillLostKeys(df: DataFrame): DataFrame = {
        var tmpDF = df
        for (key <- allHierarchies) {
            tmpDF = if (tmpDF.columns.contains(key)) tmpDF else tmpDF.withColumn(key, lit("*"))
        }
        tmpDF
    }

    private def genCartesianHierarchies(cuboid: Map[String, List[String]]) = {
        crossJoin(cuboid.values.toList)
    }

    private def crossJoin[T](list: Traversable[Traversable[T]]): Traversable[Traversable[T]] =
        list match {
            case xs :: Nil => xs map (Traversable(_))
            case x :: xs => for {
                i <- x
                j <- crossJoin(xs)
            } yield Traversable(i) ++ j
        }

    private def fillFullHierarchies(oneHierarchies: List[String], dimensions: Map[String, List[String]]): List[String] = {

        var result: List[String] = List.empty

        for (oneHierarchy <- oneHierarchies) {
            for (oneDimension <- dimensions.values) {
                if (oneDimension.contains(oneHierarchy)) {
                    for (i <- 0 to oneDimension.indexOf(oneHierarchy)) {
                        result = result :+ oneDimension(i)
                    }
                }
            }
        }
        result

    }

    //从维度层次组合中拆出时间维度层次
    private def getTimeHierarchies(oneHierarchies: List[String], dimensions: Map[String, List[String]]): List[String] = {
        var result: List[String] = List.empty
        val timeHierarchies = dimensions("time")
        for (oneHierarchy <- oneHierarchies) {
            if (timeHierarchies.contains(oneHierarchy)) {
                for (i <- 0 to timeHierarchies.indexOf(oneHierarchy)) {
                    result = result :+ timeHierarchies(i)
                }
            }
        }
        result

    }

    //MaxView所需维度组合求计算度量
    private def genMaxComputedCube(listDF: List[DataFrame]): List[DataFrame] = {
        listDF.map(df => {
            val firstRow = df.first()
            val name = firstRow.getAs[String]("DIMENSION_NAME")
            val value = firstRow.getAs[String]("DIMENSION_VALUE")
            logger.info(s"=======> name(${name}) value(${value})")
            if (name == "3-time-geo-prod") {
                logger.info("Start genMaxComputedCube !!! ")

                val arr = value.split("-")
                if (arr.size != 3) logger.error("DIMENSION_VALUE get error!")
                val time = arr(0)
                val geo = arr(1)
                val prod = arr(2)
                val geoFaGroup = getFatherHierarchiesByDiKey("geo", geo, dimensions)
                val prodFaGroup = getFatherHierarchiesByDiKey("prod", prod, dimensions)
                val selfWindow: WindowSpec = Window.partitionBy("DIMENSION_VALUE", (geoFaGroup :+ geo) ::: (prodFaGroup :+ prod): _*)
                val geoFaWindow: WindowSpec = Window.partitionBy("DIMENSION_VALUE", geoFaGroup ::: (prodFaGroup :+ prod): _*)
                val prodFaWindow: WindowSpec = Window.partitionBy("DIMENSION_VALUE", (geoFaGroup :+ geo) ::: prodFaGroup: _*)

                df
                    .withColumn("FATHER_GEO_SALES_VALUE", sum("SALES_VALUE").over(currTimeWindow(time, geoFaWindow)))
                    .withColumn("GEO_SHARE", col("SALES_VALUE")/col("FATHER_GEO_SALES_VALUE"))
                    .withColumn("FATHER_PROD_SALES_VALUE", sum("SALES_VALUE").over(currTimeWindow(time, prodFaWindow)))
                    .withColumn("PROD_SHARE", col("SALES_VALUE")/col("FATHER_PROD_SALES_VALUE"))
                    .withColumn("LAST_SALES_VALUE", sum("SALES_VALUE").over(lastTimeWindow(time, selfWindow)))
                    .withColumn("LAST_FATHER_GEO_SALES_VALUE", sum("LAST_SALES_VALUE").over(lastTimeWindow(time, geoFaWindow)))
                    .withColumn("LAST_GEO_SHARE", col("LAST_SALES_VALUE")/col("LAST_FATHER_GEO_SALES_VALUE"))
                    .withColumn("LAST_FATHER_PROD_SALES_VALUE", sum("SALES_VALUE").over(lastTimeWindow(time, prodFaWindow)))
                    .withColumn("LAST_PROD_SHARE", col("LAST_SALES_VALUE")/col("LAST_FATHER_PROD_SALES_VALUE"))
                    .withColumn("SALES_VALUE_GROWTH", when(col("LAST_SALES_VALUE") === 0.0, 0.0).otherwise(col("SALES_VALUE") - col("LAST_SALES_VALUE")))
                    .withColumn("FATHER_GEO_SALES_VALUE_GROWTH", when(col("LAST_FATHER_GEO_SALES_VALUE") === 0.0, 0.0).otherwise(col("FATHER_GEO_SALES_VALUE") - col("LAST_FATHER_GEO_SALES_VALUE")))
                    .withColumn("FATHER_PROD_SALES_VALUE_GROWTH", when(col("LAST_FATHER_PROD_SALES_VALUE") === 0.0, 0.0).otherwise(col("FATHER_PROD_SALES_VALUE") - col("LAST_FATHER_PROD_SALES_VALUE")))
                    .withColumn("SALES_VALUE_GROWTH_RATE", when(col("SALES_VALUE_GROWTH") === 0.0, 0.0).otherwise(col("SALES_VALUE_GROWTH") / col("LAST_SALES_VALUE")))
                    .withColumn("FATHER_GEO_SALES_VALUE_GROWTH_RATE", when(col("FATHER_GEO_SALES_VALUE_GROWTH") === 0.0, 0.0).otherwise(col("FATHER_GEO_SALES_VALUE_GROWTH") / col("LAST_FATHER_GEO_SALES_VALUE")))
                    .withColumn("FATHER_PROD_SALES_VALUE_GROWTH_RATE", when(col("FATHER_PROD_SALES_VALUE_GROWTH") === 0.0, 0.0).otherwise(col("FATHER_PROD_SALES_VALUE_GROWTH") / col("LAST_FATHER_PROD_SALES_VALUE")))
                    .withColumn("GEO_EI", col("GEO_SHARE")./(col("LAST_GEO_SHARE")).*(100))
                    .withColumn("PROD_EI", col("PROD_SHARE")./(col("LAST_PROD_SHARE")).*(100))
                    .withColumn("LAST_SALES_QTY", sum("SALES_QTY").over(lastTimeWindow(time, selfWindow)))
                    .withColumn("SALES_QTY_GROWTH", when(col("LAST_SALES_QTY") === 0.0, 0.0).otherwise(col("SALES_QTY") - col("LAST_SALES_QTY")))
                    .withColumn("SALES_QTY_GROWTH_RATE", when(col("SALES_QTY_GROWTH") === 0.0, 0.0).otherwise(col("SALES_QTY_GROWTH") / col("LAST_SALES_QTY")))
                    .withColumn("FATHER_GEO_SALES_QTY", sum("SALES_QTY").over(currTimeWindow(time, geoFaWindow)))
                    .withColumn("FATHER_PROD_SALES_QTY", sum("SALES_QTY").over(currTimeWindow(time, prodFaWindow)))
            } else df

        })

    }

}
