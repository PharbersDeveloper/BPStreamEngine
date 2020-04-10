package com.pharbers.StreamEngine.Jobs.GenCubeJob.strategy

import com.pharbers.util.log.PhLogable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

case class BPSHandleHiveResultStrategy(spark: SparkSession) extends BPSStrategy[DataFrame] with PhLogable {

//    val DATA_CUBE_DIMENSION_DEFINE_JSON = "./data-cube-dimension-define.json"

    var dimensions: Map[String, List[String]] = Map.empty
    var measures: List[String] = List.empty
    var allHierarchies: List[String] = List.empty
    var cuboids: List[Map[String, List[String]]] = List.empty

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
        unionListDF(cuboidsData)

    }

    //TODO:关于Dimensions和Measures的定义，是放在文件(local/hdfs)里定义，还是在启动job时以参数传递？待优化
    def initDimensions(): Map[String, List[String]] = {
        Map(
            "time" -> List("DATE", "QUARTER"), // QUARTER 是 result 原数据中没有的
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
        dimensions.values.reduce((x,y) => x ::: y)
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
            .withColumn("MONTH", col("DATE").substr(-2, 2).cast(DataTypes.IntegerType))
            .withColumn("QUARTER", ((col("MONTH") - 1) / 3) + 1)
            .withColumn("DATE", col("DATE").cast(DataTypes.IntegerType))
            .withColumn("QUARTER", col("QUARTER").cast(DataTypes.IntegerType))
            .withColumn("COUNTRY", lit("CHINA"))
            .withColumn("APEX", lit("PHARBERS"))
            .drop("MONTH")

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
            case 0 => genApexCube(df) :: Nil
            case x if x == dimensions.size => genBaseCube(df) :: Nil
            case _ => genMultiDimensionsCube(df, cuboid)
        }

    }

    def genApexCube(df: DataFrame): DataFrame = {

        val apexDF = df.groupBy("APEX").sum(measures: _*)
            .drop(measures: _*)
            .withColumnRenamed("sum(SALES_VALUE)", "SALES_VALUE")
            .withColumnRenamed("sum(SALES_QTY)", "SALES_QTY")
            .withColumn("dimension.name", lit("apex"))
            .withColumn("dimension.value", lit("*"))

        fillLostKeys(apexDF)

    }

    //TODO:根据  python 项目 gen-cube 来看，base-cube 什么也没做，待确认
    def genBaseCube(df: DataFrame): DataFrame = fillLostKeys(df.withColumn("dimension.name", lit("base")).withColumn("dimension.value", lit("*")))

    def genMultiDimensionsCube(df: DataFrame, cuboid: Map[String, List[String]]): List[DataFrame] = {

        var listDF: List[DataFrame] = List.empty
        val dimensionsName = getDimensionsName(cuboid)
        for (one_hierarchies_group <- genCartesianHierarchies(cuboid)) {
            val tmpDF = df.groupBy(one_hierarchies_group.head, one_hierarchies_group.tail.toList: _*).sum(measures: _*)
                .drop(measures: _*)
                .withColumnRenamed("sum(SALES_VALUE)", "SALES_VALUE")
                .withColumnRenamed("sum(SALES_QTY)", "SALES_QTY")
                .withColumn("dimension.name", lit(dimensionsName))
                .withColumn("dimension.value", lit(one_hierarchies_group.mkString("-")))
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

    def unionListDF(listDF: List[DataFrame]): DataFrame = {
        listDF.reduce((x, y) => x union y)
    }

}