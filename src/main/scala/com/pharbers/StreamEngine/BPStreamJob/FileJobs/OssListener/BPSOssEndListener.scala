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
        val tmp = spark.sql("select * from " + queryName).collect()
        if (tmp.length > 0 && tmp.head.getAs[Long]("count") == length) {
            job.close()
        }
    }

    override def active(s: DataFrame): Unit = LocalChannel.registerListener(this)

    override def deActive(): Unit = LocalChannel.unRegisterListener(this)
}
