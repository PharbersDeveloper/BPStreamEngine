package com.pharbers.StreamEngine.Jobs.DataCleanJob.test

import com.pharbers.StreamEngine.Jobs.DataCleanJob.EditDistanceJob.HumanReplaceJob
import com.pharbers.StreamEngine.Utils.Component2.{BPSComponentConfig, BPSConcertEntry}
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/06/24 10:48
  * @note 一些值得注意的地方
  */
object HumanReplaceTest extends App {
    val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
    val job = HumanReplaceJob(BPSComponentConfig("", "", Nil, Map()))
    val df = spark.read.format("csv")
            .option("header", "true")
            .load("s3a://ph-stream/tmp/no_replace_prod0623.csv")
    spark.sparkContext.setLogLevel("WARN")
    val humanReplaceTable = job.createHumanReplaceDfV4(df)
    job.saveTable(humanReplaceTable)
    println("over")
    spark.close()
}
