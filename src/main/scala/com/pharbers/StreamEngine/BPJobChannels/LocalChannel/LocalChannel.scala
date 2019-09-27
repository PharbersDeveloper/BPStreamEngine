package com.pharbers.StreamEngine.BPJobChannels.LocalChannel

import com.pharbers.StreamEngine.Common.StreamListener.BPStreamListener

object LocalChannel {
    var channel: Option[LocalChannel] = None

    def apply(): Unit = {
        channel = Some(new LocalChannel)
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

class LocalChannel extends Runnable {

    var lst: List[BPStreamListener] = Nil

    def registerListener(listener: BPStreamListener): Unit = lst = listener :: lst
    def trigger(): Unit = lst.foreach(_.trigger(null))

    override def run(): Unit = {
        while(true) {
            println("LocalChanel ====> " + lst)
            trigger()
            Thread.sleep(1000)
        }
    }
}