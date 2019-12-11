package com.pharbers.StreamEngine.Jobs.SqlTableJob.SqlTableListener

import java.time.Duration
import com.pharbers.StreamEngine.Jobs.SqlTableJob.SqlTableJobContainer.BPSqlTableJobContainer
import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Config.BPSConfig
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import com.pharbers.kafka.schema.HiveTask
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.spark.sql.DataFrame

import collection.JavaConverters._

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/12/11 11:14
  * @note 一些值得注意的地方
  */
case class BPSqlTableKafkaListener(job: BPSqlTableJobContainer, topic: String) extends BPStreamListener{

    val consumer: KafkaConsumer[String, HiveTask] = new PharbersKafkaConsumer[String, HiveTask](Nil).getConsumer
    consumer.subscribe(List(topic).asJava)

    override def trigger(e: BPSEvents): Unit = {
        //统一所有kafka监听时使用一个kafkaChannel来接收msg
        consumer.poll(Duration.ofSeconds(1)).asScala.foreach(x => {
            job.hiveTaskHandler(x.value())
        })
    }

    override def active(s: DataFrame): Unit =  BPSLocalChannel.registerListener(this)

    override def deActive(): Unit = {
        consumer.close()
        BPSLocalChannel.registerListener(this)
    }
}
