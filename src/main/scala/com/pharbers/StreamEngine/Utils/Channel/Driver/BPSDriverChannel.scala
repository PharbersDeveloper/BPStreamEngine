package com.pharbers.StreamEngine.Utils.Channel.Driver

import java.net.{InetAddress, InetSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Channel.ChannelComponent
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamRemoteListener
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import com.pharbers.util.log.PhLogable
import org.apache.kafka.common.config.ConfigDef
import org.json4s._
import org.json4s.jackson.Serialization.read

object BPSDriverChannel {
    var channel: Option[BPSDriverChannel] = None

//    def apply(config: Map[String, String]): BPSDriverChannel = {
    def apply(componentProperty: Component2.BPComponentConfig): BPSDriverChannel = {
        channel = channel match {
            case None => Some(new BPSDriverChannel(componentProperty))
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
        case Some(c) => c.unRegisterListener(listener)
        case None => ???
    }

    //    def waitForDriverDead() = {
    //        thread match {
    //            case Some(t) => t.join()
    //            case None => ???
    //        }
    //    }
}

// TODO 希望可以补全注释
@Component(name = "BPSDriverChannel", `type` = "BPSDriverChannel")
class BPSDriverChannel(override val componentProperty: Component2.BPComponentConfig)
    extends Runnable with PhLogable with ChannelComponent {

    lazy val host: String = InetAddress.getLocalHost.getHostAddress
//    lazy val port: Int = 56789
    lazy val port: Int = componentProperty.config("port").toInt
//    lazy val port: Int = 56780
    var lst: List[BPStreamRemoteListener] = Nil

    def registerListener(listener: BPStreamRemoteListener): Unit = lst = listener :: lst

    def unRegisterListener(listener: BPStreamRemoteListener): Unit = lst = lst.filterNot(_ == listener)

    def trigger(e: BPSEvents): Unit = lst.filter(_.hit(e)).foreach(x =>
        try {
            x.trigger(e)
        } catch {
            case e: Exception =>
                logger.error(s"listener error job: ${x.job.id}", e)
        }

    )

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

            while (iter.hasNext) {
                val item = iter.next()


                // Tests whether this key's channel is ready to accept a new socket connection
                if (item.isAcceptable) {
                    val client = driverSocket.accept()
                    logger.info("Connection Accepted: " + client.getLocalAddress)
                    client.configureBlocking(false)
                    client.register(selector, SelectionKey.OP_READ)

                } else if (item.isReadable) {
                    val client = item.channel().asInstanceOf[SocketChannel]
                    //前4位记录msg长度
                    val lengthBuffer = ByteBuffer.allocate(4)
                    var readTimes = 0
                    while (lengthBuffer.hasRemaining && readTimes < 100 && readTimes >= 0) {
                        val readCount = try{
                            client.read(lengthBuffer)
                        } catch {
                            case e: Exception =>
                                logger.error(e.getMessage, e)
                                readTimes = 100
                                -1
                        }
                        readTimes += 1
                        Thread.sleep(10)
                        if(readCount == -1){
                            logger.debug("socket channel被关闭了")
                            readTimes = -1
                        }
                    }

                    val length = if(readTimes > 0){
                        val lengthBytes = lengthBuffer.array()
                        ((lengthBytes(0) & 0xff) << 24) |
                                ((lengthBytes(1) & 0xff) << 16) |
                                ((lengthBytes(2) & 0xff) << 8) |
                                (lengthBytes(3) & 0xff)
                    } else {
                        logger.info("cancel channel")
                        -1
                    }
                    logger.info("Message length: " + length)
                    if(length > 0) {
                        try {
                            readMsgFromChannel(length, client)
                        } catch {
                            case e: Exception =>
                                logger.error(e.getMessage, e)
                        }
                    } else {
                        item.cancel()
                    }

                }
                iter.remove()
            }
        }
    }

    private def readMsgFromChannel(length: Int, client: SocketChannel): Unit ={
        var readTimes = 0
        val buffer = ByteBuffer.allocate(length)
        while (buffer.hasRemaining && readTimes < 200) {
            client.read(buffer)
            readTimes += 1
//            Thread.sleep(10)
            Thread.sleep(componentProperty.config("sleep").toInt)
        }
        if(readTimes >= 200) return
        val result = new String(buffer.array()).trim()
        logger.info("Message received: " + result)

        if (result.equals("alfred end")) {
            client.close()
            logger.info("It's time to close connection")
            logger.info("Server will keep running. Try running client again to establish new connection")
        }

        implicit val formats: DefaultFormats.type = DefaultFormats
        try {
            val event = read[BPSEvents](result)
            trigger(event)
        } catch {
            case e: com.fasterxml.jackson.core.JsonParseException =>
                logger.error(e.getMessage, e)
        }
    }

    override val channelName: String = "driver channel"
    override def createConfigDef(): ConfigDef = new ConfigDef()
}
