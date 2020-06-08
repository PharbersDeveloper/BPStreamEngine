package com.pharbers.StreamEngine

import com.pharbers.StreamEngine.Utils.Strategy.Log.BPSLogContext
import com.pharbers.StreamEngine.Utils.Component.ComponentContext
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

//object main extends App {
//    BPSLogContext(null)
//    ComponentContext.init()
//    ThreadExecutor.waitForShutdown()
//}

object main_oom {
    def main(args: Array[String]): Unit = {
        BPSConcertEntry.start()
        ThreadExecutor.waitForShutdown()
    }
}
