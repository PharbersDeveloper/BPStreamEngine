package com.pharbers.StreamEngine.Utils.Channel.Local

import java.util.concurrent.LinkedBlockingQueue

import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Channel.ChannelComponent
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import com.pharbers.util.log.PhLogable
import org.apache.kafka.common.config.ConfigDef


object BPSLocalChannel {
    var channel: Option[BPSLocalChannel] = None

//    def apply(config: Map[String, String]): BPSLocalChannel = {
    def apply(componentProperty: Component2.BPComponentConfig): BPSLocalChannel = {
        channel = channel match {
            case None =>
                val channel = new BPSLocalChannel(componentProperty)
                ThreadExecutor().execute(channel)
                Some(channel)
            case _ => channel
        }
        channel.get
    }

    def registerListener(listener: BPStreamListener): Unit = channel match {
        case Some(c) => c.registerListener(listener)
        case None => sys.error("未创建BPSLocalChannel")

    }

    def unRegisterListener(listener: BPStreamListener): Unit = channel match {
        case Some(c) => c.unRegisterListener(listener)
        case None => sys.error("未创建BPSLocalChannel")
    }

    def offer(event: BPSEvents): Boolean = channel match {
        case Some(c) => c.offer(event)
        case None => sys.error("未创建BPSLocalChannel")

    }
}

// TODO 希望可以补全注释
@Component(name = "BPSLocalChannel", `type` = "BPSLocalChannel")
class BPSLocalChannel(override val componentProperty: Component2.BPComponentConfig)
    extends Runnable with PhLogable with ChannelComponent {

    var lst: List[BPStreamListener] = Nil
    //todo: 使用配置
    val events: LinkedBlockingQueue[BPSEvents] = new LinkedBlockingQueue[BPSEvents](10000)

    def registerListener(listener: BPStreamListener): Unit = lst = listener :: lst

    def unRegisterListener(listener: BPStreamListener): Unit = lst = lst.filterNot(_ == listener)

    def offer(event: BPSEvents): Boolean = events.offer(event)

    def trigger(): Unit = {
        //没有时会阻塞，根据业务需要之后可以修改为pull。但是资源消耗会增加
        val event = events.take()
        lst.filter(_.hit(event)).foreach(x => {
            try {
                x.trigger(event)
            } catch {
                case e: Exception => logger.error(e.getMessage, e)
            }

        })
    }

    override def run(): Unit = {
        logger.info("Local Channel Server")
        while (true) {
            trigger()
        }
    }

    override def createConfigDef(): ConfigDef = new ConfigDef()

    override val channelName: String = "local channel"
}
