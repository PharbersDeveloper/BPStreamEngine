package com.pharbers.StreamEngine.Utils.Job

import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/10/22 15:36
  * @note 一些值得注意的地方
  */
trait BPDynamicStreamJob extends BPStreamJob {
    def registerListeners(listener: BPStreamListener)
    def handlerExec(handler: BPSEventHandler)

    override def close(): Unit = {
        //todo: 要先在JobHandler中注销才行
        super.close()
    }
}
