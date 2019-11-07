package com.pharbers.StreamEngine.Utils.Channel.Driver

import java.net.{InetAddress, InetSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel

import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamRemoteListener
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import com.pharbers.util.log.PhLogable
import org.json4s._
import org.json4s.jackson.Serialization.read

object BPSDriverChannel {
    var channel: Option[BPSDriverChannel] = None

    def apply(config: Map[String, String]): BPSDriverChannel = {
        channel = channel match {
            case None => Some(new BPSDriverChannel(config))
            case _ => channel
        }
        ThreadExecutor().execute(channel.get)
        channel.get
    }

    def registerListener(listener: BPStreamRemoteListener): Unit = channel match {
            case Some(c) => c.registerListener(listener)
            case None => ???
        }

    def unRegisterListener(listener: BPStreamRemoteListener): Unit = channel match {
            case Some(c) => c.lst = c.lst.filterNot(_ == listener)
            case None => ???
        }

//    def waitForDriverDead() = {
//        thread match {
//            case Some(t) => t.join()
//            case None => ???
//        }
//    }
}

@Component(name = "BPSDriverChannel", `type` = "BPSDriverChannel")
class BPSDriverChannel(config: Map[String, String]) extends Runnable with PhLogable{

    lazy val host: String = InetAddress.getLocalHost.getHostAddress
    lazy val port: Int = 56789
    var lst: List[BPStreamRemoteListener] = Nil

    def registerListener(listener: BPStreamRemoteListener): Unit = lst = listener :: lst
    def trigger(e: BPSEvents): Unit = lst.filter(_.hit(e)).foreach(_.trigger(e))

    override def run(): Unit = {
        val selector: Selector = Selector.open // selector is open here
        val driverSocket: ServerSocketChannel = {
            val driverAddr = new InetSocketAddress(host, port)
            val tmp = ServerSocketChannel.open
            tmp.bind(driverAddr).configureBlocking(false)
            tmp
        }

        val ops: Int = driverSocket.validOps
        val selectKy: SelectionKey = driverSocket.register(selector, ops, null)
        logger.info("Driver Channel Server")
        while (true) {
            // Selects a set of keys whose corresponding channels are ready for I/O operations
            selector.select()

            // token representing the registration of a SelectableChannel with a Selector
            val keys = selector.selectedKeys()
            val iter = keys.iterator()

            while (iter.hasNext()) {
                val item = iter.next()

                // Tests whether this key's channel is ready to accept a new socket connection
                if (item.isAcceptable()) {
                    val client = driverSocket.accept()
                    logger.info("Connection Accepted: " + client.getLocalAddress())
                    client.configureBlocking(false)
                    client.register(selector, SelectionKey.OP_READ)

                } else if (item.isReadable()) {
                    val client =  item.channel().asInstanceOf[SocketChannel]
                    // TODO: 分包读取的机制
                    val Buffer = ByteBuffer.allocate(4096)
                    if (client.read(Buffer) > 0) {
                        val result = new String(Buffer.array()).trim()
                        logger.info("Message received: " + result)

                        if (result.equals("alfred end")) {
                            client.close()
                            logger.info("It's time to close connection")
                            logger.info("Server will keep running. Try running client again to establish new connection")
                        }

                        implicit val formats = DefaultFormats
                        val event = read[BPSEvents](result)
                        trigger(event)
                    }
                }
                iter.remove()
            }
        }
    }
}
