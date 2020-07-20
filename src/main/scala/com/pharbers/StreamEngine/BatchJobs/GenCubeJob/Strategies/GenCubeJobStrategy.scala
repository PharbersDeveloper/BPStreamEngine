package com.pharbers.StreamEngine.BatchJobs.GenCubeJob.Strategies

import com.pharbers.StreamEngine.Utils.Log.PhLogable
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

class GenCubeJobStrategy(spark: SparkSession) extends PhLogable {

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
            "prod" -> List("COMPANY", "MKT", "PRODUCT_NAME", "MOLE_NAME") // MKT 是 result 原数据中没有的
        )
    }

    private def initMeasures(): List[String] = {
        List("SALES_VALUE", "SALES_QTY")
    }

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

    //补齐所需列 QUARTER COUNTRY MKT
    private def dataCleaning(df: DataFrame): DataFrame = {

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
                    .withColumn("FATHER_GEO_SALES_VALUE", sum("SALES_VALUE").over(geoFaWindow))
                    .withColumn("GEO_SHARE", col("SALES_VALUE")/col("FATHER_GEO_SALES_VALUE"))
                    .withColumn("FATHER_PROD_SALES_VALUE", sum("SALES_VALUE").over(prodFaWindow))
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
                    .withColumn("FATHER_GEO_SALES_QTY", sum("SALES_QTY").over(geoFaWindow))
                    .withColumn("FATHER_PROD_SALES_QTY", sum("SALES_QTY").over(prodFaWindow))
            } else df

        })

    }

    //从维度层次组合中拆出时间维度层次
    private def getFatherHierarchiesByDiKey(diKey: String, oneHierarchy: String, dimensions: Map[String, List[String]]): List[String] = {
        var result: List[String] = List.empty
        val hierarchies = dimensions(diKey)
        if (hierarchies.head == oneHierarchy) return result
        if (hierarchies.contains(oneHierarchy)) {
            for (i <- 0 until hierarchies.indexOf(oneHierarchy)) {
                result = result :+ hierarchies(i)
            }
        }
        result

    }

    //由于时间维度的三个层级YEAR/QUARTER/MONTH，在某一层级中只能求上期(lastYEAR/lastQUARTER/lastMONTH)的增长/增长率
    private def lastTimeWindow(currentTimeHierarchy: String, currentWindow: WindowSpec): WindowSpec = {
        currentTimeHierarchy match {
            case "YEAR" => currentWindow
                    .orderBy(col("YEAR").cast(DataTypes.IntegerType))
                    .rangeBetween(-1, -1)
            case "QUARTER" => currentWindow
                    .orderBy(col("YEAR").cast(DataTypes.IntegerType).*(100).+(col("QUARTER").cast(DataTypes.IntegerType).*(25)))
                    .rangeBetween(-25, -25)
            case "MONTH" => currentWindow
                    .orderBy(to_date(col("YEAR").cast(DataTypes.IntegerType).*(100).+(col("MONTH").cast(DataTypes.IntegerType)).cast(DataTypes.StringType), "yyyyMM").cast("timestamp").cast("long"))
                    .rangeBetween(-86400 * 31, -86400 * 28)
        }
    }

}
