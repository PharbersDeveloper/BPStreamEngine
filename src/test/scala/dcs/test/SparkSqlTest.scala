package dcs.test

import com.pharbers.StreamEngine.Jobs.DataCleanJob.EditDistanceJob.BPEditDistance
import com.pharbers.StreamEngine.Utils.Component2.{BPSComponentConfig, BPSConcertEntry}
import com.pharbers.StreamEngine.Utils.Strategy.Schema.SchemaConverter
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import dcs.test.SpecUnitTransform.spark
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/01/03 16:18
  * @note 一些值得注意的地方
  */
object SparkSqlTest extends App {
    val spark = BPSparkSession(null)
    val df = spark.read.parquet("/common/public/cpa/0.0.11")
            .filter("company != 'Janssen'")
            .withColumn("version", lit("0.0.12"))

    val jassen = spark.read
            .format("csv")
            .option("header", true)
            .option("delimiter", ",")
            .load("/user/alex/jobs/bef4434e-a6b5-4680-b586-3d8a7db70f17/0058a115-8cdf-4395-8853-022a851ffdfe/contents",
                "/user/alex/jobs/bef4434e-a6b5-4680-b586-3d8a7db70f17/287a1f5b-eb6e-4d61-94c7-d72c14a7e681/contents")
            .withColumn("version", lit("0.0.12"))
            .withColumn("COMPANY", lit("Janssen"))
            .withColumn("SOURCE", lit("CPA&GYC"))


    df.unionByName(jassen)
            .repartition()
            .write
            .option("path", "/common/public/cpa/0.0.12")
            .saveAsTable("cpa")
}

object SparkSql extends App {
//    val conf = new Configuration
//    conf.set("fs.defaultFS", "hdfs://192.168.100.14:8020")
//    val hdfs = FileSystem.newInstance(conf)
//    hdfs.delete(new Path("hdfs://192.168.100.14:8020/test/testBPStream/sandBoxRes/metadata"), true)
val sc = BPSConcertEntry.queryComponentWithId("schema convert").get.asInstanceOf[SchemaConverter]
    val spark = SparkSession.builder().config(new SparkConf().setMaster("local[*]")).enableHiveSupport().getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", sys.env("AWS_ACCESS_KEY_ID"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", sys.env("AWS_SECRET_ACCESS_KEY_ID"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")
    //    val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
//    spark.conf.set("spark.sql.files.maxPartitionBytes", "8388608")
//    spark.conf.set("spark.sql.files.openCostInBytes", "1048576")
    spark.sparkContext.setLogLevel("INFO")
    spark.sparkContext.addJar("./jars/mysql-connector-java-5.1.44.jar")

    spark.read.parquet("s3a://ph-stream/common/public/human_replace/0.0.14")
            .filter("min != '硝普钠硝普钠50mg注射剂1湖南恒生制药有限公司'")
            .withColumn("version", lit("0.0.15"))
            .write
            .mode("overwrite")
            .option("path", "s3a://ph-stream/common/public/human_replace/0.0.15")
            .saveAsTable("human_replace")





    //    val schemaDf = spark.read.parquet("/test/testBPStream/ossJobRes/400wDfsTest")
    //    schemaDf.write.mode("overwrite").parquet("/test/testBPStream/ossJobRes/800wDfs2")
    //    val df = spark.readStream
    ////            .format("csv")
    ////            .option("header", true)
    ////            .option("delimiter", ",")
    ////            .load("/jobs/a77038b2-5721-42bd-b9b4-07f07731dca6/6eeb4c6a-fb4f-498f-a763-864e64e324cf/contents")
    //            .schema(schemaDf.schema)
    //            .parquet("/test/testBPStream/ossJobRes/400wDfsTest")
    //
    //    df.writeStream.format("parquet")
    //            .partitionBy("jobId")
    //            .option("checkpointLocation", "/test/testBPStream/ossJobRes/checkpoint")
    //            .start("/test/testBPStream/ossJobRes/400wContents")
        Thread.sleep(1000 * 60 * 30)
}

object SpecUnitTransform extends App {
    //    val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]

    val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession].spark
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val checkFun = udf((s1: String, s2: String) => check(s1, s2))
    val df = spark.sql("SELECT distinct COL_NAME,  ORIGIN,  CANDIDATE,  ORIGIN_MOLE_NAME, ORIGIN_PRODUCT_NAME, ORIGIN_SPEC, ORIGIN_DOSAGE, ORIGIN_PACK_QTY, ORIGIN_MANUFACTURER_NAME FROM cpa_no_replace")
            .filter("COL_NAME == 'SPEC'")
            //            .withColumn("id", monotonically_increasing_id())
            .withColumn("can_replace", checkFun($"ORIGIN", $"CANDIDATE" (0)))


    df.write.mode("overwrite").option("path", "s3a://ph-stream/common/public/spec_unit_transform").saveAsTable("spec_unit_transform")

    def check(s1: String, s2: String): Boolean = {
        checkSep(s1, s2)
    }

    //切分并且转换单位
    def checkSep(s1: String, s2: String): Boolean = {
        val list1 = s1.toUpperCase().split("[^A-Za-z0-9_.\\u4e00-\\u9fa5]", -1).map(unitTransform).sorted
        val list2 = s2.toUpperCase().split("[^A-Za-z0-9_.\\u4e00-\\u9fa5]", -1).map(unitTransform).sorted
        list1.sameElements(list2)
    }

    def unitTransform(s: String): String = {
        val unit = s.replaceAll("[0-9.]", "")
        try {
            unit match {
                case "G" => (s.substring(0, s.length - 1).toDouble * 1000).toInt.toString + "MG"
                case _ => s
            }
        } catch {
            case _: Exception => s
        }
    }
}

object saveNoReplaceDf extends App {
    val minColumns = List("MOLE_NAME", "PRODUCT_NAME", "SPEC", "DOSAGE", "PACK_QTY", "MANUFACTURER_NAME")
    //    val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]

    val spark =  BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession].spark
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    case class replaceLog(min: String, cols: Map[String, String])

    val replaceDf = spark.sql("SELECT distinct COL_NAME,  ORIGIN,  DEST,  ORIGIN_MOLE_NAME, ORIGIN_PRODUCT_NAME, ORIGIN_SPEC, ORIGIN_DOSAGE, ORIGIN_PACK_QTY, ORIGIN_MANUFACTURER_NAME FROM cpa_replace")
            .withColumn("min", concat(minColumns.map(x => col(s"ORIGIN_$x")): _*))
            .withColumn("cols", map($"COL_NAME", $"DEST"))
            .select("min", "cols")
            .as[replaceLog]
            .groupByKey(x => x.min)
            .mapGroups((min, rows) => {
                (min, rows.foldLeft(Map[String, String]())((l, r) => l ++ r.cols))
            }).toDF("min", "cols")
            .selectExpr("min" +: minColumns.map(x => s"cols['$x'] as $x"): _*)

    val noReplaceDf = spark.sql("SELECT distinct COL_NAME, ORIGIN, CANDIDATE, ORIGIN_MOLE_NAME, ORIGIN_PRODUCT_NAME, ORIGIN_SPEC, ORIGIN_DOSAGE, ORIGIN_PACK_QTY, ORIGIN_MANUFACTURER_NAME FROM cpa_no_replace")
            .withColumn("min", concat(minColumns.map(x => col(s"ORIGIN_$x")): _*))
            .join(replaceDf.withColumnRenamed("min", "min1"), $"min" === $"min1", "left")

    noReplaceDf.write.parquet("/user/dcs/test/no_replace_prod")
}

object createAutoHumanReplaceDf extends App {
    val minColumns = List("MOLE_NAME", "PRODUCT_NAME", "SPEC", "DOSAGE", "PACK_QTY", "MANUFACTURER_NAME")

    val mappingConfig: Map[String, String] = Map(
        "PRODUCT_NAME" -> "PROD_NAME_CH",
        "SPEC" -> "SPEC",
        "DOSAGE" -> "DOSAGE",
        "PACK_QTY" -> "PACK",
        "MANUFACTURER_NAME" -> "MNF_NAME_CH"
    )

    val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession].spark
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val noReplaceDf = minColumns.foldLeft(spark.read.parquet("/user/dcs/test/no_replace_prod"))((df, name) => {
        df.withColumn(name, when(col(name).isNotNull, col(name)).otherwise(col(s"ORIGIN_$name")))
    })
            .withColumn("cols", concat_ws(31.toChar.toString, minColumns.map(x => col(x)): _*))
            .withColumn("id", monotonically_increasing_id())

    val prod = spark.sql("select * from prod")
            .withColumn("prod_cols", concat_ws(31.toChar.toString, $"MOLE_NAME_CH", $"PROD_NAME_CH", $"SPEC", $"DOSAGE", $"PACK", $"MNF_NAME_CH", $"MNF_NAME_EN"))
            .withColumnRenamed("SPEC", "prod_SPEC")
            .withColumnRenamed("DOSAGE", "prod_DOSAGE")


    val joinDf = noReplaceDf.join(prod, $"MOLE_NAME" === $"MOLE_NAME_CH", "left")

    val canReplaceDf = joinDf.groupByKey(x => x.getAs[Long]("id")).mapGroups((_, rows) => {
        val rowWithNum = rows.map(x => (x, joinFunc(x))).toList
        val max = rowWithNum.maxBy(x => x._2)
        if (rowWithNum.count(x => x._2 == max._2) == 1) {
            (max._1.getAs[String]("min"),
                    max._1.getAs[String]("MOLE_NAME_CH"),
                    max._1.getAs[String]("PROD_NAME_CH"),
                    max._1.getAs[String]("prod_SPEC"),
                    max._1.getAs[String]("prod_DOSAGE"),
                    max._1.getAs[String]("PACK"),
                    max._1.getAs[String]("MNF_NAME_CH"))
        } else ("", "", "", "", "", "", "")
    })
            .toDF("min", "MOLE_NAME", "PRODUCT_NAME", "SPEC", "DOSAGE", "PACK_QTY", "MANUFACTURER_NAME")
            .filter($"min" =!= "")

    canReplaceDf.show(false)
    canReplaceDf.write.mode("overwrite").parquet("/user/dcs/test/can_replace")

    def joinFunc(row: Row): Int = {
        val list1 = row.getAs[String]("cols").split(31.toChar.toString)
        val list2 = row.getAs[String]("prod_cols").split(31.toChar.toString)
        list1.intersect(list2).length
    }
}

object read extends App {
    val mappingConfig: Map[String, List[String]] = Map(
        "PRODUCT_NAME" -> List("PROD_NAME_CH"),
        "SPEC" -> List("SPEC"),
        "DOSAGE" -> List("DOSAGE"),
        "PACK_QTY" -> List("PACK"),
        "MANUFACTURER_NAME" -> List("MNF_NAME_CH", "MNF_NAME_EN")
    )

    //    val spark = SparkSession.builder().config(new SparkConf().setMaster("local[8]")).enableHiveSupport().getOrCreate()
    val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession].spark
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val bPEditDistance = new BPEditDistance(null, BPSComponentConfig("", "", Nil, Map("jobId" -> "test", "dataSets" -> "")))
    val noReplaceDf = spark.sql("select distinct ORIGIN_MOLE_NAME, ORIGIN_PRODUCT_NAME, ORIGIN_SPEC, ORIGIN_DOSAGE, ORIGIN_PACK_QTY, ORIGIN_MANUFACTURER_NAME from cpa_no_replace")
            .withColumn("id", monotonically_increasing_id())
            .withColumn("cols", map(mappingConfig.keySet.toList.flatMap(x => List(lit(x), col(s"ORIGIN_$x"))): _*))
    val prod = spark.sql("select MOLE_NAME_CH, PROD_NAME_CH, SPEC, DOSAGE, PACK, MNF_NAME_CH, MNF_NAME_EN  from prod")
            .withColumn("prod_cols", map(mappingConfig.values.toList.flatten.flatMap(x => List(lit(x), col(x))): _*))

    case class joinRow(id: Long, cols: Map[String, String], prodCols: Map[String, String], moleName: String)

    noReplaceDf.join(prod, "ORIGIN_MOLE_NAME = MOLE_NAME_CH")
            .selectExpr("id", "cols", "prod_cols", "ORIGIN_MOLE_NAME as mole_name")
            .as[joinRow]
            .groupByKey(x => x.id)
//            .mapGroups((id, rows) => {
//
//            })

    def getDistance(inputWord: String, targetWord: String): Boolean = {
        val s1 = inputWord.replaceAll(" ", "").toUpperCase
        val s2 = targetWord.replaceAll(" ", "").toUpperCase
        inputWord != targetWord && (s1 == s2 || checkSep(inputWord, targetWord))
    }

    private def checkSep(s1: String, s2: String): Boolean = {
        val list1 = s1.toUpperCase().split("[^A-Za-z0-9_.\\u4e00-\\u9fa5]", -1).sorted
        val list2 = s2.toUpperCase().split("[^A-Za-z0-9_.\\u4e00-\\u9fa5]", -1).sorted
        list1.sameElements(list2)
    }
}

