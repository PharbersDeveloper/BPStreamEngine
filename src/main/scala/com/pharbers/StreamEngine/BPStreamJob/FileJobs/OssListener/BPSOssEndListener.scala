package com.pharbers.StreamEngine.BPStreamJob.FileJobs.OssListener

import com.pharbers.StreamEngine.BPJobChannels.LocalChannel.LocalChannel
import com.pharbers.StreamEngine.BPStreamJob.BPStreamJob
import com.pharbers.StreamEngine.Common.Events
import com.pharbers.StreamEngine.Common.StreamListener.BPStreamListener
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.Logger

case class BPSOssEndListener(
                                val spark: SparkSession,
                                val job: BPStreamJob,
                                val queryName: String,
                                val length: Int
                            ) extends BPStreamListener {
    val log = LogManager.getLogger(this)
    override def trigger(e: Events): Unit = {
        val tmp = spark.sql("select * from " + queryName).collect()
        log.debug(s"queryName:$queryName")
        log.debug(s"length:$length")
        if (tmp.length > 0) log.debug(s"count: ${tmp.head.getAs[Long]("count")}")
        if (tmp.length > 0 && tmp.head.getAs[Long]("count") == length) {
            job.close()
        }
    }

    override def active(s: DataFrame): Unit = LocalChannel.registerListener(this)

    override def deActive(): Unit = LocalChannel.unRegisterListener(this)
}
