package com.pharbers.StreamEngine.Jobs.OssJob.OssListener

import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Job.BPStreamJob
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
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

    override def active(s: DataFrame): Unit = BPSLocalChannel.registerListener(this)

    override def deActive(): Unit = BPSLocalChannel.unRegisterListener(this)
}
