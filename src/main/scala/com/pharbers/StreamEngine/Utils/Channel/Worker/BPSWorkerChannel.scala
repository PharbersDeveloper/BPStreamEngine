package com.pharbers.StreamEngine.Utils.Channel.Worker

import java.net.{InetAddress, InetSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel


object BPSWorkerChannel {
//    var host: Broadcast[String] = _
    val port: Int = 56789

    def apply(host: String): BPSWorkerChannel = {
        val tmp = new BPSWorkerChannel(host, port)
        tmp.connect()
        tmp
    }
//    def init(hostBroadcast: Broadcast[String]): Unit ={
//        host = hostBroadcast
//    }
}

class BPSWorkerChannel(host: String, port: Int) extends Serializable {

    lazy val addr = new InetSocketAddress("192.168.100.115", port)

    var client: Option[SocketChannel] = None

    def connect(): Unit = {
        try {
            client = Some(SocketChannel.open(addr))
        }catch {
            case e: Exception => throw new Exception(s"error~~~worker~~~~host:${addr.getHostString} $host, name: ${addr.getPort}", e)
        }
        //todo: log
        println("Connecting to Server on port 55555 ...")
    }

    def pushMessage(msg: String): Unit = {
        val message = msg.getBytes()
        val buffer = ByteBuffer.wrap(message)
        client match {
            case Some(c) => c.write(buffer)
            case None => ???
        }

        buffer.clear()
        Thread.sleep(1000)
    }

    def close(): Unit = client.get.close()
}
