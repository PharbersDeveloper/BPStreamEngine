package com.pharbers.StreamEngine.Jobs.PyJob.Listener

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.HDFS.BPSHDFSFile
import com.pharbers.StreamEngine.Jobs.PyJob.BPSPythonJob
import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener

/** 监控 PythonJob 的执行进度, 并在完成后关闭 PythonJob
 *
 * @param job           要监控的 PythonJob
 * @param spark         Spark Session 实例
 * @param rowLength     可能会处理的总行数
 * @param rowRecordPath 当前处理的行数记录位置
 * @author clock
 * @version 0.0.1
 * @since 2019/11/08 13:40
 */
case class BPSProgressListenerAndClose(override val job: BPSPythonJob,
                                       spark: SparkSession,
                                       rowLength: Long,
                                       rowRecordPath: String) extends BPStreamListener {

    override def trigger(e: BPSEvents): Unit = {
        val rows = BPSHDFSFile.readHDFS(rowRecordPath).map(_.toLong).sum

        logger.debug("=========> Total Row " + rowLength)
        logger.debug("=====>" + rows)
        if (rows >= rowLength) {
            logger.info("******>" + rows)
            job.close()
        }
    }

    override def active(s: DataFrame): Unit = BPSLocalChannel.registerListener(this)

    override def deActive(): Unit = BPSLocalChannel.unRegisterListener(this)
}

