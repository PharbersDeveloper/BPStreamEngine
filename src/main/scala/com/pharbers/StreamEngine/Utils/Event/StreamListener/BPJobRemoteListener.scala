package com.pharbers.StreamEngine.Utils.Event.StreamListener
import com.pharbers.StreamEngine.Utils.Channel.Driver.BPSDriverChannel
import com.pharbers.StreamEngine.Utils.Event.{BPSEvents, BPSTypeEvents}
import com.pharbers.StreamEngine.Utils.Job.BPStreamJob
import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数 case class，scala集合类，scala基本类
  * @author dcs
  * @version 0.0
  * @since 2020/04/07 15:58
  * @note 如果在泛型方法中使用，需要定义泛型时继承Manifest
  */
class BPJobRemoteListener[T](override val job: BPStreamJob, hitTypes: List[String])(trigFunc: BPSTypeEvents[T] => Unit) extends BPStreamRemoteListener{

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
    def apply[T](job: BPStreamJob, hitTypes: List[String])(trigFunc: BPSTypeEvents[T] => Unit): BPJobRemoteListener[T] =
        new BPJobRemoteListener(job, hitTypes)(trigFunc)
}
