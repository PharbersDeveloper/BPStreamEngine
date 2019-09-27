package com.pharbers.StreamEngine.BPJobChannels.DriverChannel

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel

import com.pharbers.StreamEngine.Common.Events
import com.pharbers.StreamEngine.Common.StreamListener.BPStreamRemoteListener
import org.json4s._
import org.json4s.jackson.Serialization.read

object DriverChannel {
    var channel: Option[DriverChannel] = None

    def apply(): Unit = {
        channel = Some(new DriverChannel)
        new Thread(channel.get).start()
    }

    def registerListener(listener: BPStreamRemoteListener): Unit = channel match {
            case Some(c) => c.registerListener(listener)
            case None => ???
        }

    def unRegisterListener(listener: BPStreamRemoteListener): Unit = channel match {
            case Some(c) => c.lst = c.lst.filterNot(_ == listener)
            case None => ???
        }
}

class DriverChannel extends Runnable {

    lazy val host: String = "192.168.100.115"
    lazy val port: Int = 56789
    var lst: List[BPStreamRemoteListener] = Nil

    def registerListener(listener: BPStreamRemoteListener): Unit = lst = listener :: lst
    def trigger(e: Events): Unit = lst.filter(_.hit(e)).foreach(_.trigger(e))

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

        println("Driver Channel Server")
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
                    println("Connection Accepted: " + client.getLocalAddress())
                    client.configureBlocking(false)
                    client.register(selector, SelectionKey.OP_READ)

                } else if (item.isReadable()) {
                    val client =  item.channel().asInstanceOf[SocketChannel]
                    // TODO: 分包读取的机制
                    val Buffer = ByteBuffer.allocate(2048)
                    if (client.read(Buffer) > 0) {
                        val result = new String(Buffer.array()).trim()
                        println("Message received: " + result)

                        if (result.equals("alfred end")) {
                            client.close()
                            println("It's time to close connection")
                            println("Server will keep running. Try running client again to establish new connection")
                        }

                        implicit val formats = DefaultFormats
                        val event = read[Events](result)
                        trigger(event)
                    }
                }
                iter.remove()
            }
        }
    }
}
