package com.pharbers.StreamEngine.Jobs.GenCubeJob.strategy

import com.pharbers.util.log.PhLogable
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

class BPSGenCubeToEsStrategy(spark: SparkSession) extends BPSStrategy[DataFrame] with PhLogable {

    val DEFAULT_INDEX_NAME: String = "cube"

    var dimensions: Map[String, List[String]] = Map.empty
    var measures: List[String] = List.empty
    var allHierarchies: List[String] = List.empty
    var cuboids: List[Map[String, List[String]]] = List.empty
    var unifiedColumns: Array[String] = Array.empty

    override def convert(data: DataFrame): DataFrame = {

        logger.info("Start exec gen-cube strategy.")

        /**
          * hive 中的 result 的 keys
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


        val cleanData = dataCleaning(data)

        dimensions = initDimensions()
        measures = initMeasures()
        allHierarchies = "APEX" :: initAllHierarchies()
        cuboids = initCuboids()

        val cuboidsData = genCuboidsData(cleanData)

        //加一个函数只对所需维度组合求计算度量
        val maxComputedCube = genMaxComputedCube(cuboidsData)

        //尝试分批写入
        writeEsListDF(maxComputedCube)

    }

    //TODO:关于Dimensions和Measures的定义，是放在文件(local/hdfs)里定义，还是在启动job时以参数传递？待优化
    def initDimensions(): Map[String, List[String]] = {
        Map(
            "time" -> List("YEAR", "QUARTER", "MONTH"), // "YEAR", "QUARTER", "MONTH" 是 result 原数据中没有的, 由DATE(YM)转变
            "geo" -> List("COUNTRY", "PROVINCE", "CITY"), // COUNTRY 是 result 原数据中没有的
            "prod" -> List("COMPANY", "MKT", "PRODUCT_NAME", "MOLE_NAME") // MKT 是 result 原数据中没有的
        )
    }

    def initMeasures(): List[String] = {
        List("SALES_VALUE", "SALES_QTY")
    }

    //TODO:check dimensions and measures 是否在 数据源中

    def initCuboids(): List[Map[String, List[String]]] = {
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

    def initAllHierarchies(): List[String] = {
        dimensions.values.reduce((x, y) => x ::: y)
    }

    //补齐所需列 QUARTER COUNTRY MKT
    def dataCleaning(df: DataFrame): DataFrame = {

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
        //删除不需列 MONTH
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

        mergeDF1 union mergeDF2

    }

    //对每个维度的排列组合进行聚合
    def genCuboidsData(df: DataFrame): List[DataFrame] = {

        var listDF: List[DataFrame] = List.empty

        for (cuboid <- cuboids) {
            listDF = listDF ::: genCuboidData(df, cuboid)
        }

        listDF

    }

    def genCuboidData(df: DataFrame, cuboid: Map[String, List[String]]): List[DataFrame] = {

        cuboid.size match {
//            case 0 => genApexCube(df) :: Nil
            case x if x == dimensions.size => genMultiDimensionsCube(df, cuboid)
//            case _ => genMultiDimensionsCube(df, cuboid)
            case _ => Nil
        }

    }

    def genApexCube(df: DataFrame): DataFrame = {

        val apexDF = df.groupBy("APEX").sum(measures: _*)
            .drop(measures: _*)
            .withColumnRenamed("sum(SALES_VALUE)", "SALES_VALUE")
            .withColumnRenamed("sum(SALES_QTY)", "SALES_QTY")
            .withColumn("DIMENSION_NAME", lit("apex"))
            .withColumn("DIMENSION_VALUE", lit("*"))
            .withColumn("SALES_RANK", dense_rank.over(Window.partitionBy("APEX").orderBy(desc("SALES_VALUE"))))  //以SALES_VALUE降序排序

        val apexCube = fillLostKeys(apexDF)
        unifiedColumns = apexCube.columns
        apexCube

    }

    //TODO:不用baseCube了，直接求multiDimensionsCube
    def genBaseCube(df: DataFrame): DataFrame = fillLostKeys(
        df
            .withColumn("DIMENSION_NAME", lit("base"))
            .withColumn("DIMENSION_VALUE", lit("*"))
    )

    def genMultiDimensionsCube(df: DataFrame, cuboid: Map[String, List[String]]): List[DataFrame] = {

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
                .withColumn("SALES_RANK", dense_rank.over(Window.partitionBy("DIMENSION_VALUE", time_group: _*).orderBy(desc("SALES_VALUE")))) //以时间维度分partition才有排名的意义，受限于时间维度上年/季/月层次
            listDF = listDF :+ fillLostKeys(tmpDF)
        }
        listDF

    }

    def getDimensionsName(cuboid: Map[String, List[String]]): String = {
        if (cuboid.isEmpty) return "*"
        cuboid.size + "-" + cuboid.keySet.mkString("-")
    }

    def fillLostKeys(df: DataFrame): DataFrame = {
        var tmpDF = df
        for (key <- allHierarchies) {
            tmpDF = if (tmpDF.columns.contains(key)) tmpDF else tmpDF.withColumn(key, lit("*"))
        }
        tmpDF
    }

    def genCartesianHierarchies(cuboid: Map[String, List[String]]) = {
        crossJoin(cuboid.values.toList)
    }

    def crossJoin[T](list: Traversable[Traversable[T]]): Traversable[Traversable[T]] =
        list match {
            case xs :: Nil => xs map (Traversable(_))
            case x :: xs => for {
                i <- x
                j <- crossJoin(xs)
            } yield Traversable(i) ++ j
        }

    def fillFullHierarchies(oneHierarchies: List[String], dimensions: Map[String, List[String]]): List[String] = {

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
    def getTimeHierarchies(oneHierarchies: List[String], dimensions: Map[String, List[String]]): List[String] = {
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
    def genMaxComputedCube(listDF: List[DataFrame]): List[DataFrame] = {
        listDF.map(df => {
            val firstRow = df.first()
            val name = firstRow.getAs[String]("DIMENSION_NAME")
            val value = firstRow.getAs[String]("DIMENSION_VALUE")
            logger.info(s"=======> name(${name}) value(${value})")
            if (name == "3-time-geo-prod" && value == "MONTH-CITY-MOLE_NAME") {
                logger.info("Start genMaxComputedCube !!! ")
                val newDf = df.withColumn("DATE", concat(col("YEAR"), col("MONTH")))
                //TODO:数据是否正确需要核对
                PhMaxDashboardWindowFunc(newDf)
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
            } else df

        })

    }

    //尝试分批append写入
    def writeEsListDF(listDF: List[DataFrame]): DataFrame = {

        for (df <- listDF) {
            df.write
                .format("es")
                //                .option("es.write.operation", "upsert")
                .mode("append")
                .save(DEFAULT_INDEX_NAME)
        }

        spark.emptyDataFrame

    }

}
