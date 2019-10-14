package com.pharbers.StreamEngine.OssJobs.FileJobs.OssListener

import com.pharbers.StreamEngine.BPJobChannels.LocalChannel.LocalChannel
import com.pharbers.StreamEngine.BPStreamJob.BPStreamJob
import com.pharbers.StreamEngine.Common.Event.BPSEvents
import com.pharbers.StreamEngine.Common.Event.StreamListener.BPStreamListener
import org.apache.spark.sql.{DataFrame, SparkSession}

case class BPSOssEndListener(
                                val spark: SparkSession,
                                val job: BPStreamJob,
                                val queryName: String,
                                val length: Int
                            ) extends BPStreamListener {
    override def trigger(e: BPSEvents): Unit = {
        val tmp = spark.sql("select * from " + queryName).collect()
        //todo: log
        println(s"queryName:$queryName")
        println(s"length:$length")
        if (tmp.length > 0) println(s"count: ${tmp.head.getAs[Long]("count")}")
        if (tmp.length > 0 && tmp.head.getAs[Long]("count") == length) {
            job.close()
        }
    }

    override def active(s: DataFrame): Unit = LocalChannel.registerListener(this)

    override def deActive(): Unit = LocalChannel.unRegisterListener(this)
}