package com.pharbers.StreamEngine.Jobs.PyJob.Listener

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Jobs.PyJob.BPSPythonJob
import com.pharbers.StreamEngine.Jobs.PyJob.Py4jServer.BPSPy4jServer
import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener

/** 监控 PythonJob 的执行进度, 并在完成后关闭 PythonJob
 *
 * @param job           要监控的 PythonJob
 * @param spark         可能会处理的总行数
 * @param rowLength     可能会处理的总行数
 * @param rowRecordPath 可能会处理的总行数
 * @author clock
 * @version 0.1
 * @since 2019/11/08 13:40
 */
case class BPSProgressListenerAndClose(override val job: BPSPythonJob,
                                       spark: SparkSession,
                                       rowLength: Long,
                                       rowRecordPath: String) extends BPStreamListener {

    override def trigger(e: BPSEvents): Unit = {
        val rows = try {
            spark.sparkContext
                    .textFile(rowRecordPath)
                    .collect()
                    .map(_.toLong)
                    .sum
        } catch {
            case _: Exception => 0L
        }

        logger.debug("=========> Total Row " + rowLength)
        logger.debug("=====>" + rows)
        if (rows >= rowLength) {
            logger.debug("******>" + rows)
            job.close()
        }
    }

    override def active(s: DataFrame): Unit = BPSLocalChannel.registerListener(this)

    override def deActive(): Unit = BPSLocalChannel.unRegisterListener(this)
}

