package com.pharbers.StreamEngine.Utils.Channel.Local

import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener

object BPSLocalChannel {
    var channel: Option[BPSLocalChannel] = None

    def apply(): Unit = {
        channel = Some(new BPSLocalChannel)
        new Thread(channel.get).start()
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