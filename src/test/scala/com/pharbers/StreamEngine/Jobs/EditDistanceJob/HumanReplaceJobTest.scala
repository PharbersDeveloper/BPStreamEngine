package com.pharbers.StreamEngine.Jobs.EditDistanceJob

import com.pharbers.StreamEngine.Utils.Component2.{BPSComponentConfig, BPSConcertEntry}
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat}
import org.scalatest.FunSuite

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/04/23 13:23
  * @note 一些值得注意的地方
  */
class HumanReplaceJobTest extends FunSuite{
    test("test createHumanReplaceDf"){
        val localSpark = SparkSession.builder().config(new SparkConf().setMaster("local[8]")).enableHiveSupport().getOrCreate()
        val job = HumanReplaceJob(BPSComponentConfig("", "", Nil, Map()))
        import localSpark.implicits._
        val df = localSpark.read.format("csv")
                .option("header", "true")
                .load("file:///D:\\文件\\excel数据表\\产品\\humanReplace\\CPA_no_replace 第9次提数0424.csv")
        localSpark.sparkContext.setLogLevel("WARN")
        val humanReplaceTable = job.createHumanReplaceDf(df)
        val minColumns = List("ORIGIN_MOLE_NAME", "ORIGIN_PRODUCT_NAME", "ORIGIN_SPEC", "ORIGIN_DOSAGE", "ORIGIN_PACK_QTY", "ORIGIN_MANUFACTURER_NAME")
        val inDf = df.filter($"PackID" =!= "#N/A").na.fill("")
                .withColumn("min1", concat(minColumns.map(x => col(x)): _*))
        val joinDf = humanReplaceTable
//                .withColumnRenamed("min", "min1")
                .join(inDf, $"min" === $"min1", "right")

        joinDf.cache()
        assert(joinDf.filter($"min".isNotNull).count() == df.filter($"PackID" =!= "#N/A").count())
        assert(joinDf.filter("ORIGIN_MOLE_NAME1 != MOLE_NAME").count() == 0)
        assert(joinDf.filter("ORIGIN_PRODUCT_NAME1 != PRODUCT_NAME").count() == 0)
        assert(joinDf.filter("ORIGIN_SPEC1 != SPEC").count() == 0)
        assert(joinDf.filter("ORIGIN_DOSAGE1 != DOSAGE").count() == 0)
        assert(joinDf.filter("ORIGIN_PACK_QTY1 != PACK_QTY").count() == 0)
        assert(joinDf.filter("ORIGIN_MANUFACTURER_NAME1 != MANUFACTURER_NAME").count() == 0)
        println("over")
        spark.close()
    }
}

object HumanReplaceJobRun extends App {
    val localSpark = SparkSession.builder().config(new SparkConf().setMaster("local[8]")).enableHiveSupport().getOrCreate()
    val job = HumanReplaceJob(BPSComponentConfig("", "", Nil, Map()))
    val df = localSpark.read.format("csv")
            .option("header", "true")
            .load("file:///D:\\文件\\excel数据表\\产品\\humanReplace\\CPA_no_replace 第9次提数0424.csv")
    localSpark.sparkContext.setLogLevel("WARN")
    val humanReplaceTable = job.createHumanReplaceDf(df)
    job.saveTable(humanReplaceTable)
    println("over")
    spark.close()
}

object HumanReplaceMarge extends App{
    val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
    import spark.spark.implicits._
    val job = HumanReplaceJob(BPSComponentConfig("", "", Nil, Map()))
    spark.sparkContext.setLogLevel("WARN")
    val autoHumanReplaceDf = spark.read.parquet("/user/dcs/test/can_replace").distinct()
    val specUnitTransformDf = spark.sql("select * from spec_unit_transform where can_replace = true")
            .withColumn("min", concat(job.minColumns.map(x => col(s"ORIGIN_$x")): _*))
            .withColumn("ORIGIN_SPEC", $"CANDIDATE"(0))
            .selectExpr("min" +: job.minColumns.map(x => s"ORIGIN_$x as $x"): _*)

    val autoHumanReplaceTable = autoHumanReplaceDf.selectExpr("min as min1")
            .join(specUnitTransformDf, $"min1" === $"min", "right")
            .filter("min1 is null")
            .drop("min1")
            .unionByName(autoHumanReplaceDf)
            .cache()

    val oldDf = job.getOldTable
    val humanReplaceTable = job.margeTable(job.margeMinColumns(autoHumanReplaceTable), job.margeMinColumns(oldDf))

//    job.saveTable(humanReplaceTable)
    println("over")
    spark.close()
}
