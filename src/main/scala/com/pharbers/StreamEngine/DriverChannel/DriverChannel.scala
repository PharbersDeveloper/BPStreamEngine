package com.pharbers.StreamEngine.DriverChannel

import java.net.{InetSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel

import com.pharbers.StreamEngine.Common.Events
import org.json4s._
import org.json4s.jackson.Serialization.read

object DriverChannel {
    def apply(): Unit = new Thread(new DriverChannel).start()
}

class DriverChannel extends Runnable {

    lazy val host: String = "192.168.100.115"
    lazy val port: Int = 56789

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

        println("i'm a server and i'm waiting for new connection and buffer select...")
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
                    val Buffer = ByteBuffer.allocate(2048)
                    if (client.read(Buffer) > 0) {
                        val result = new String(Buffer.array()).trim()
                        println("Message received: " + result)

                        implicit val formats = DefaultFormats
                        val event = read[Events](result)

                        if (result.equals("alfred end")) {
                            client.close()
                            println("It's time to close connection")
                            println("Server will keep running. Try running client again to establish new connection")
                        }
                    }
                }
                iter.remove()
            }
        }
    }
}
