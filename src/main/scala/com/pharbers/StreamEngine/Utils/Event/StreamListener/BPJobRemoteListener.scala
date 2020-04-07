package com.pharbers.StreamEngine.Utils.Event.StreamListener
import com.pharbers.StreamEngine.Utils.Channel.Driver.BPSDriverChannel
import com.pharbers.StreamEngine.Utils.Event.{BPSEvents, BPSTypeEvents}
import com.pharbers.StreamEngine.Utils.Job.BPStreamJob
import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/04/07 15:58
  * @note 一些值得注意的地方
  */
class BPJobRemoteListener[T: ClassTag](override val job: BPStreamJob, hitTypes: List[String])(trigFunc: BPSTypeEvents[T] => Unit) extends BPStreamRemoteListener{

    override def hit(e: BPSEvents): Boolean = hitTypes.contains(e.`type`)

    override def trigger(e: BPSEvents): Unit = {
        trigFunc(BPSTypeEvents[T](e))
    }

    override def active(s: DataFrame): Unit = {
        BPSDriverChannel.unRegisterListener(this)
    }

    override def deActive(): Unit = {
        BPSDriverChannel.registerListener(this)
    }
}

object BPJobRemoteListener {
    def apply[T: ClassTag](job: BPStreamJob, hitTypes: List[String])(trigFunc: BPSTypeEvents[T] => Unit): BPJobRemoteListener[T] =
        new BPJobRemoteListener(job, hitTypes)(trigFunc)
}
