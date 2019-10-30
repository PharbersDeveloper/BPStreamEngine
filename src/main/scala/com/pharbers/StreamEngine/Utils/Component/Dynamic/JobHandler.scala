package com.pharbers.StreamEngine.Utils.Component.Dynamic

import com.pharbers.StreamEngine.Utils.StreamJob.BPDynamicStreamJob
import org.apache.kafka.common.config.ConfigDef

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/10/22 15:30
  * @note 一些值得注意的地方
  */
trait JobHandler extends Runnable{
    val configDef: ConfigDef = new ConfigDef()

    def add(jobMsg: JobMsg)
    def getJob(id: String): Option[BPDynamicStreamJob]
    def finish(id: String)
}
