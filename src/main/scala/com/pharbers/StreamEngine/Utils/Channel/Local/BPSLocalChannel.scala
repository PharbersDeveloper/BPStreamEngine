package com.pharbers.StreamEngine.Utils.Channel.Local

import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor

object BPSLocalChannel {
    var channel: Option[BPSLocalChannel] = None

    def apply(config: Map[String, String]): BPSLocalChannel = {
        channel = channel match {
            case None => Some(new BPSLocalChannel)
            case _ => channel
        }
        ThreadExecutor().execute(channel.get)
        channel.get
    }

    def registerListener(listener: BPStreamListener): Unit = channel match {
        case Some(c) => c.registerListener(listener)
        case None => ???
    }

    def unRegisterListener(listener: BPStreamListener): Unit = channel match {
        case Some(c) => c.lst = c.lst.filterNot(_ == listener)
        case None => ???
    }
}

@Component(name = "BPSLocalChannel", `type` = "BPSLocalChannel")
class BPSLocalChannel extends Runnable {

    var lst: List[BPStreamListener] = Nil

    def registerListener(listener: BPStreamListener): Unit = lst = listener :: lst
    def trigger(): Unit = lst.foreach(_.trigger(null))

    override def run(): Unit = {
        while(true) {
            trigger()
            Thread.sleep(1000)
        }
    }
}