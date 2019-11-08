package com.pharbers.StreamEngine.Jobs.PyJob.Listener

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Jobs.PyJob.BPSPythonJob
import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener

/** 监控 PythonJob 的执行进度, 并在完成后关闭 PythonJob
 *
 * @param job 要监控的 PythonJob
 * @param sumRow 可能会处理的总行数
 * @author clock
 * @version 0.1
 * @since 2019/11/08 13:40
 */
case class BPSProgressListenerAndClose(override val job: BPSPythonJob,
                                       query: StreamingQuery,
                                       sumRow: Long) extends BPStreamListener {
//    val query: StreamingQuery = job.outputStream.head

    override def trigger(e: BPSEvents): Unit = {
        val cumulative = query.recentProgress.map(_.numInputRows).sum

        if (query.lastProgress != null) {
            logger.debug("---->" + query.lastProgress.numInputRows)
        }
        logger.debug("=========> Total Row " + sumRow)
        logger.debug("=====>" + cumulative)
        if (cumulative >= sumRow) {
            logger.debug("******>" + cumulative)
            job.close()
        }
    }

    override def active(s: DataFrame): Unit = BPSLocalChannel.registerListener(this)

    override def deActive(): Unit = BPSLocalChannel.unRegisterListener(this)
}

