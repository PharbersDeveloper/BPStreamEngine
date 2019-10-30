package com.pharbers.StreamEngine.Jobs.OssJob

import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.StreamJob.{BPDynamicStreamJob, BPStreamJob}
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import org.apache.spark.sql.{DataFrame, SparkSession}

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/10/23 16:43
  * @note 一些值得注意的地方
  */
class DynamicJobDemo(override val spark: SparkSession, override val strategy: BPSKfkJobStrategy,
                     override val id: String, config: Map[String, String]) extends BPDynamicStreamJob{
    override type T = BPSKfkJobStrategy

    override def registerListeners(listener: BPStreamListener): Unit = {
        if(!listeners.contains(listener)){
            listeners = listeners :+ listener
        }
        println(s"注册listener:$listener")
        listener.active(null)
        println(s"触发listener:$listener")
        listener.trigger(null)
    }

    override def handlerExec(handler: BPSEventHandler): Unit = {
        if(!handlers.contains(handler)){
            handlers = handlers :+ handler
        }
        println(s"指定怎么运行handler:$handlers")
    }

    override def open(): Unit = {
        println(s"$id:open")
    }

    override def exec(): Unit = {
        println(s"$id:exec")
        listeners.filter(x => x.hit(null)).foreach(x => x.trigger(null))
    }

    override def close(): Unit = {
        handlers.foreach(_.close())
        listeners.foreach(_.deActive())
        outputStream.foreach(_.stop())
        println(s"$id:close")
    }
}
// args + dependencyArgs + Map[String, String]
class DynamicListenerDemo(id: String, override val job: BPStreamJob, config: Map[String, String]) extends BPStreamListener{

    override def trigger(e: BPSEvents): Unit = {
        println(s"$id:trigger")
    }

    override def active(s: DataFrame): Unit = {
        println(s"$id:active")
    }

    override def deActive(): Unit = {
        println(s"$id:deActive")
    }
}
