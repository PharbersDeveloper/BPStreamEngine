package com.pharbers.StreamEngine

import com.pharbers.StreamEngine.Utils.Log.BPSLogContext
import com.pharbers.StreamEngine.Utils.Component.ComponentContext
import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor

object main extends App {
    BPSLogContext.init()
    ComponentContext.init()
    ThreadExecutor.waitForShutdown()
}

object test extends App{
    val spark = BPSparkSession()
    val df = spark.read.parquet("/common/public/cpa/0.0.6.1")
//    df.write.saveAsTable("cpa2")
//    df.write.partitionBy("YEAR", "MONTH").saveAsTable("cpa3")
}


