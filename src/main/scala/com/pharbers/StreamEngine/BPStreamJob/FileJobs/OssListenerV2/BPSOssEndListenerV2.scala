package com.pharbers.StreamEngine.BPStreamJob.FileJobs.OssListenerV2

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDate

import com.pharbers.StreamEngine.BPJobChannels.LocalChannel.LocalChannel
import com.pharbers.StreamEngine.BPStreamJob.BPStreamJob
import com.pharbers.StreamEngine.Common.Events
import com.pharbers.StreamEngine.Common.StreamListener.BPStreamListener
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/10/11 16:42
  * @note 一些值得注意的地方
  */
class BPSOssEndListenerV2(
                                val spark: SparkSession,
                                val job: BPStreamJob,
                                val queryName: String,
                                val endTimestamp: Timestamp,
                                val query: StreamingQuery
                        ) extends BPStreamListener {
    override def trigger(e: Events): Unit = {
        val progress = query.recentProgress.filter(x => x.eventTime.containsKey("max"))
        //todo: log
        println(s"queryName:$queryName")
        println(s"end:$endTimestamp")
        if (progress.nonEmpty) {
            val date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS Z")
                    .parse(progress.last.eventTime.getOrDefault("max", "1970-01-01T00:00:00.000Z").replace("Z", " UTC"))
            println(s"watermark: ${date.toString}")
            if (date.getTime >= endTimestamp.getTime) {
                job.close()
            }
        }
    }

    override def active(s: DataFrame): Unit = LocalChannel.registerListener(this)

    override def deActive(): Unit = LocalChannel.unRegisterListener(this)
}
