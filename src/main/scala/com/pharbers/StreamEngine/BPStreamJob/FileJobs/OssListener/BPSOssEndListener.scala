package com.pharbers.StreamEngine.BPStreamJob.FileJobs.OssListener

import com.pharbers.StreamEngine.BPJobChannels.LocalChannel.LocalChannel
import com.pharbers.StreamEngine.BPStreamJob.BPStreamJob
import com.pharbers.StreamEngine.Common.Events
import com.pharbers.StreamEngine.Common.StreamListener.BPStreamListener
import org.apache.spark.sql.{DataFrame, SparkSession}

case class BPSOssEndListener(
                                val spark: SparkSession,
                                val job: BPStreamJob,
                                val queryName: String,
                                val length: Int
                            ) extends BPStreamListener {
    import spark.implicits._
    override def trigger(e: Events): Unit = {
        println("End Trigger")
    }

    override def active(s: DataFrame): Unit = LocalChannel.registerListener(this)

    override def deActive(s: DataFrame): Unit = LocalChannel.unRegisterListener(this)
}
