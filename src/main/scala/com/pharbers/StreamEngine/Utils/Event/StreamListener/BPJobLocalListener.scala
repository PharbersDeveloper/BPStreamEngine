package com.pharbers.StreamEngine.Utils.Event.StreamListener

import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Event.{BPSEvents, BPSTypeEvents}
import com.pharbers.StreamEngine.Utils.Job.BPStreamJob
import org.apache.spark.sql.DataFrame

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数 case class，scala集合类，scala基本类.case class中在json中可能没有的属性需要使用option.属性默认值无效
  * @author dcs
  * @version 0.0
  * @since 2020/04/08 15:14
  * @note 如果在泛型方法中使用，需要定义泛型时继承Manifest
  */
class BPJobLocalListener[T](override val job: BPStreamJob, hitTypes: List[String], needHitNull: Boolean = false)(trigFunc: BPSTypeEvents[T] => Unit) extends BPStreamRemoteListener{

    val chanel: BPSLocalChannel = BPSConcertEntry.queryComponentWithId("driver channel").get.asInstanceOf[BPSLocalChannel]

    override def hit(e: BPSEvents): Boolean = (e != null || needHitNull) && hitTypes.contains(e.`type`)

    override def trigger(e: BPSEvents): Unit = {
        trigFunc(BPSTypeEvents[T](e))
    }

    override def active(s: DataFrame): Unit = {
        chanel.registerListener(this)
    }

    override def deActive(): Unit = {
        chanel.unRegisterListener(this)
    }
}

object BPJobLocalListener {
    def apply[T](job: BPStreamJob, hitTypes: List[String], needHitNull: Boolean = false)(trigFunc: BPSTypeEvents[T] => Unit): BPJobLocalListener[T] =
        new BPJobLocalListener(job, hitTypes)(trigFunc)
}
