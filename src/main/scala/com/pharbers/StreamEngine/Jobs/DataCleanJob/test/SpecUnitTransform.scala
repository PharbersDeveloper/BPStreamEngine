package com.pharbers.StreamEngine.Jobs.DataCleanJob.test

import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.spark.sql.functions.udf

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/06/22 12:59
  * @note 一些值得注意的地方
  */
object SpecUnitTransform extends App {
    val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession].spark
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    val tableName = "pfizer_test"

    val checkFun = udf((s1: String, s2: Seq[String]) => check(s1, s2))
    val df = spark.sql(s"SELECT distinct COL_NAME,  ORIGIN,  CANDIDATE,  ORIGIN_MOLE_NAME, ORIGIN_PRODUCT_NAME, ORIGIN_SPEC, ORIGIN_DOSAGE, ORIGIN_PACK_QTY, ORIGIN_MANUFACTURER_NAME FROM ${tableName}_no_replace")
            .filter("COL_NAME == 'SPEC'")
            //            .withColumn("id", monotonically_increasing_id())
            .withColumn("can_replace", checkFun($"ORIGIN", $"CANDIDATE"))


    df.write.mode("overwrite").option("path", "s3a://ph-stream/common/public/spec_unit_transform").saveAsTable("spec_unit_transform")

    def check(s1: String, s2: Seq[String]): String = {
        checkSep(s1, s2)
    }

    //切分并且转换单位
    def checkSep(s1: String, s2: Seq[String]): String = {
        val list1 = s1.toUpperCase().split("[^A-Za-z0-9_.\\u4e00-\\u9fa5]", -1).map(unitTransform).filter(x => x != "")
        s2.find(x => {
            val list = x.toUpperCase().split("[^A-Za-z0-9_.\\u4e00-\\u9fa5]", -1).map(unitTransform)
            val length = list1.intersect(list).length
            length >= list.length || length >= list1.length
        }).getOrElse("")
    }

    def unitTransform(s: String): String = {
        val num = "[0-9.]".r.findAllIn(s).mkString("")
        val unit = s.substring(s.indexOf(num)).replaceAll("[0-9.]", "")
        try {
            unit match {
                case "G" => (num.toDouble * 1000).toString + "MG"
                case _ => num.toDouble.toString + unit
            }
        } catch {
            case _: Exception => s
        }
    }
}

object test extends App{
    checkSep("0.15625g(阿莫西林0.125g,克拉维酸钾31.25mg)", "156.25mg" :: Nil)
    def checkSep(s1: String, s2: Seq[String]): String = {
        val list1 = s1.toUpperCase().split("[^A-Za-z0-9_.\\u4e00-\\u9fa5]", -1).map(unitTransform)
        s2.find(x => {
            val list = x.toUpperCase().split("[^A-Za-z0-9_.\\u4e00-\\u9fa5]", -1).map(unitTransform)
            list1.intersect(list).length >= list.length
        }).getOrElse("")
    }

    def unitTransform(s: String): String = {
        val num = "[0-9.]".r.findAllIn(s).mkString("")
        val unit = s.substring(s.indexOf(num)).replaceAll("[0-9.]", "")
        try {
            unit match {
                case "G" => (num.toDouble * 1000).toString + "MG"
                case _ => num.toDouble.toString + unit
            }
        } catch {
            case _: Exception => s
        }
    }
}
