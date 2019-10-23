package com.pharbers.StreamEngine.Utils.Component.Dynamic

import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.StreamJob.BPDynamicStreamJob

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/10/22 15:42
  * @note 一些值得注意的地方
  */
trait BPDynamicStreamJobBuilder {
    def buildJob(jobMsg: JobMsg, job: BPDynamicStreamJob): BPDynamicStreamJob
    def buildListener(jobMsg: JobMsg, listener: BPStreamListener): BPStreamListener
    def buildHandler(jobMsg: JobMsg, handler: BPSEventHandler): BPSEventHandler
}
