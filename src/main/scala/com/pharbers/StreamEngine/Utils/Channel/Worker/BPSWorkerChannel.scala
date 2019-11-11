package com.pharbers.StreamEngine.Utils.Channel.Worker

import java.net.{InetAddress, InetSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

import com.pharbers.util.log.PhLogable


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

// TODO 希望可以补全注释，因为我不知道这是干什么的
class BPSWorkerChannel(host: String, port: Int) extends Serializable with PhLogable {

    lazy val addr = new InetSocketAddress(host, port)

    var client: Option[SocketChannel] = None

    def connect(): Unit = {
        try {
            client = Some(SocketChannel.open(addr))
        } catch {
            case e: Exception => throw new Exception(s"error~~~worker~~~~host:${addr.getHostString} $host, name: ${addr.getPort}", e)
        }
        logger.info("Connecting to Server on port 55555 ...")
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
