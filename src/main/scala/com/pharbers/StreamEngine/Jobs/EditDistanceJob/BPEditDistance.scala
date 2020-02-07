package com.pharbers.StreamEngine.Jobs.EditDistanceJob

import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSCommonJoBStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/** 功能描述
  *
  * @author dcs
  * @version 0.0
  * @since 2020/02/04 13:38
  * @note 一些值得注意的地方
  */
case class BPEditDistance(jobContainer: BPSJobContainer, spark: SparkSession, config: Map[String, String]) extends BPStreamJob {
    override type T = BPSCommonJoBStrategy
    override val strategy: BPSCommonJoBStrategy = BPSCommonJoBStrategy(config)
    val jobId: String = strategy.getJobId
    val runId: String = strategy.getRunId
    override val id: String = jobId

    override def open(): Unit = {
        inputStream = Some(spark.sql("select * from cpa"))
        //        inputStream = jobContainer.inputStream
    }


    override def exec(): Unit = {
        //todo: 配置传入
        val mappingDf = spark.sql("select * from prod")
        inputStream match {
            case Some(in) => check(in, mappingDf)
            case _ =>
        }
    }

    override def close(): Unit = {
        super.close()
        jobContainer.finishJobWithId(id)
    }

    def check(in: DataFrame, checkDf: DataFrame): Unit = {
        import spark.sqlContext.implicits._
        //todo: 配置传入
        val mapping = Map(
            "ATC" -> List("ATC1_CODE", "ATC2_CODE", "ATC3_CODE"),
            "PRODUCT_NAME" -> List("PROD_NAME_CH", "PROD_NAME"),
            "SPEC" -> List("SPEC"),
            "DOSAGE" -> List("DOSAGE"),
            "PACK_QTY" -> List("PACK"),
            "MANUFACTURER_NAME" -> List("CORP_NAME_CH", "CORP_NAME_EN", "MNF_NAME_CH11", "MNF_NAME_CH12")
        )
        val inDfRename = in.columns.foldLeft(in)((l, r) =>
            l.withColumnRenamed(r, s"in_$r"))
        val checkDfRename = checkDf.columns.foldLeft(checkDf)((l, r) =>
            l.withColumnRenamed(r, s"check_$r"))

        val joinDf = inDfRename
                .withColumn("id", monotonically_increasing_id())
                .join(checkDfRename, col("in_MOLE_NAME") === col("check_MOLE_NAME_CH"), "left")
                .na.fill("")
        //                .repartitionByRange(100, col("id"))
        val distanceDf = mapping.foldLeft(joinDf)((l, r) => getColumnDistance(l, r._1, r._2))
        //同一个id取编辑距离和最小的一行
        val usdCheck = udf((map: Map[String, Int]) => BPEditDistance.checkFunc(map))
        val res = distanceDf
                .filter("check_MOLE_NAME_CH != ''")
                .groupByKey(x => x.getAs[Long]("id"))
                .mapGroups((id, row) => row.reduce((l, r) => {
                    val leftSum = mapping.keySet.foldLeft(0)((x, y) => x + l.getAs[Int](s"${y}_distance"))
                    val rightSum = mapping.keySet.foldLeft(0)((x, y) => x + r.getAs[Int](s"${y}_distance"))
                    if (leftSum > rightSum) r else l
                }))(RowEncoder(distanceDf.schema))
                //判断编辑距离是否在误差范围
                .withColumn("check", usdCheck(map(mapping.keySet.toList.flatMap(x => List(col(s"in_$x"), col(s"${x}_distance"))): _*)))


        res.write
                .mode("overwrite")
                .parquet("/user/dcs/test/BPStreamEngine/cpa_edit_distance")
    }


    def getColumnDistance(df: DataFrame, column: String, checkColumns: List[String]): DataFrame = {
        val udfTest = udf((x: String, y: String) => BPEditDistance.getDistance(x, y))
        df.na.fill("")
                .withColumn(s"${column}_distance", sort_array(array(checkColumns.map(x =>
                    udfTest(col(s"in_$column"), col(s"check_$x"))): _*))(0))
    }

}

object BPEditDistance extends Serializable {
    def checkFunc(map: Map[String, Int]): Boolean = {
        map.forall(r => (r._1.length / 5 + 1) >= r._2)
    }

    def getDistance(inputWord: String, targetWord: String): Int = {
        val resContainer = Array.fill(inputWord.length + 1, targetWord.length + 1)(-1)
        distance(inputWord, targetWord, inputWord.length, targetWord.length, resContainer)
    }

    private def distance(x: String, y: String, i: Int, j: Int, resContainer: Array[Array[Int]]): Int = {
        if (resContainer(i)(j) != -1) return resContainer(i)(j)
        if (i == 0 || j == 0) {
            return Math.max(i, j)
        }

        val replaceRes = if (x.charAt(i - 1) == y.charAt(j - 1)) distance(x, y, i - 1, j - 1, resContainer) else distance(x, y, i - 1, j - 1, resContainer) + 1
        val removeRes = Math.min(distance(x, y, i, j - 1, resContainer), distance(x, y, i - 1, j, resContainer)) + 1
        val res = Math.min(removeRes, replaceRes)
        resContainer(i)(j) = res
        res
    }
}


object test extends App {

    import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession

    val spark = BPSparkSession()
    //    val jobContainer = new BPSJobContainer() {
    //        override type T = BPSCommonJoBStrategy
    //        override val strategy: BPSCommonJoBStrategy = null
    //        override val id: String = ""
    //        override val spark: SparkSession = null
    //    }
    //    jobContainer.inputStream = Some(spark.sql("select * from cpa"))

    val job = BPEditDistance(null, spark, Map("jobId" -> "test", "runId" -> "test"))
    job.open()
    job.exec()
}
