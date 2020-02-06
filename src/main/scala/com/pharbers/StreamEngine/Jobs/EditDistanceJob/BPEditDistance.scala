package com.pharbers.StreamEngine.Jobs.EditDistanceJob

import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSCommonJoBStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

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
        inputStream = jobContainer.inputStream
    }


    override def exec(): Unit = {
        val mappingDf = spark.sql("select * from prod")
        inputStream match {
            case Some(in) =>
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

        val joinDf = in.columns.foldLeft(in)((l, r) => l.withColumnRenamed(r, s"in_$r"))
                .withColumn("id", monotonically_increasing_id())
                .join(checkDf.columns.foldLeft(checkDf)((l, r) => l.withColumnRenamed(r, s"check_$r")), in("in_MOLE_NAME") === checkDf("check_MOLE_NAME_CH"))
                .na.fill("")
        val distanceDf = mapping.foldLeft(joinDf)((l, r) => getColumnDistance(l, r._1, r._2))
        distanceDf.groupByKey(x => x.getAs[String]("id")).mapGroups((id, row) => row)
    }

//    def checkRow(row: Row): Row = {
//        new GenericRowWithSchema()
//        row
//    }

    def getColumnDistance(df: DataFrame, column: String, checkColumns: List[String]): DataFrame ={
        val udfTest = udf((x: String, y: String) => getDistance(x, y))
        df.na.fill("")
                .withColumn(s"${column}_distance", sort_array(array(checkColumns.map(x => udfTest(col(s"in_$column"), col(x))): _*))(0))
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
