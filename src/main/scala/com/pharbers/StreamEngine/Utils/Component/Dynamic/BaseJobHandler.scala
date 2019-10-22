package com.pharbers.StreamEngine.Utils.Component.Dynamic

import java.time.Duration

import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Component.ComponentContext
import com.pharbers.StreamEngine.Utils.Component.Node.NodeMsgHandler
import com.pharbers.StreamEngine.Utils.Config.AppConfig
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.StreamJob.BPDynamicStreamJob
import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import com.pharbers.kafka.schema.BPJob
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.json4s._
import org.json4s.jackson.Serialization.read
import collection.JavaConverters._
import scala.reflect.runtime.universe

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/10/22 16:22
  * @note
  */
@Component(name = "BaseJobHandler", `type` = "JobHandler")
private class BaseJobHandler(config: Map[String, String], nodeHandler: NodeMsgHandler, jobBuilder: BPDynamicStreamJobBuilder) extends JobHandler{
    final val TOPIC_CONFIG_KEY = "topic"
    final val TOPIC_CONFIG_DOC = "kafka topic"
    configDef.define(TOPIC_CONFIG_KEY, Type.STRING, "stream_job_submit", Importance.HIGH, TOPIC_CONFIG_DOC)
    private val handlerConfig: AppConfig = new AppConfig(configDef,  config.asJava)
    private var jobs: Map[String, BPDynamicStreamJob] = Map.empty

    override def add(jobMsg: JobMsg): Unit = {
        if(!check(jobMsg)){
            return
        }
        val args = jobMsg.args.map(x => {
            if(x.startsWith("$")){
                ComponentContext().getComponent[AnyRef](args)
            } else {
                x
            }
        }) :: jobMsg.dependencyArgs.map(x => {
            if(x.contains('.')){
                getFieldMirror(jobs(x.split('.').head), x.split('.').tail.head)
            } else {
                getFieldMirror(jobs(jobMsg.dependencies.head), x)
            }
        })
        jobMsg.`type` match {
            case "job" => addJob(jobMsg, args)
            case "listener" => addListener(jobMsg, args)
            case "handler" => addHandler(jobMsg, args)
            case _ =>
        }
    }

    override def getJob(id: String): Option[BPDynamicStreamJob] = jobs.get(id)

    override def finish(id: String): Unit = {
        getJob(id) match {
            case Some(job) =>
                job.close()
                nodeHandler.find(id).get.jobs.foreach(x => {
                    val child = nodeHandler.find(x._1).get
                    if(child.dependencyStop == id) finish(child.id)
                })
            case _ =>
        }
    }

    override def run(): Unit = {
        val consumer = new PharbersKafkaConsumer[String, BPJob](List(handlerConfig.getString(TOPIC_CONFIG_KEY))).getConsumer
        while (true){
            consumer.poll(Duration.ofSeconds(1)).asScala.foreach(x => {
                implicit val formats: DefaultFormats.type = DefaultFormats
                x.value().getType.toString match {
                    case "add" => add(read[JobMsg](x.value().getJob.toString))
                    case "stop" => finish(x.value().getJob.toString)
                    case _ =>
                }
            })
        }
    }

    private def addJob(jobMsg: JobMsg, args: Seq[Any]): Unit ={
        val job = jobBuilder.buildJob(jobMsg.id, getMethodMirror(jobMsg.classPath)(args: _*).asInstanceOf[BPDynamicStreamJob])
        synchronized(this){
            jobs = jobs ++ Map(jobMsg.id -> job)
        }
        job.open()
        job.exec()
    }

    private def addListener(jobMsg: JobMsg, args: Seq[Any]): Unit ={
        val listener = jobBuilder.buildListener(jobMsg.id, getMethodMirror(jobMsg.classPath)(args: _*).asInstanceOf[BPStreamListener])
        jobMsg.dependencies.foreach(x => {
            jobs(x).listeners = jobs(x).listeners :+ listener
            jobs(x).registerListeners(listener)
        })
    }

    private def addHandler(jobMsg: JobMsg, args: Seq[Any]): Unit ={
        val handler = jobBuilder.buildHandler(jobMsg.id, getMethodMirror(jobMsg.classPath)(args: _*).asInstanceOf[BPSEventHandler])
        jobMsg.dependencies.foreach(x => {
            jobs(x).handlers = jobs(x).handlers :+ handler
            jobs(x).handlerExec(handler)
        })
    }

    private def check(jobMsg: JobMsg): Boolean ={
        //todo: 验证依赖参数是否指定正确,验证依赖的job是否存在,class是不是BPDynamicStreamJob, 是listener和handler时是否有依赖
        true
    }

    private def getMethodMirror(reference: String): universe.MethodMirror = {
        val m = universe.runtimeMirror(getClass.getClassLoader)
        val classSy = m.classSymbol(Class.forName(reference))
        val cm = m.reflectClass(classSy)
        val ctor = classSy.toType.decl(universe.termNames.CONSTRUCTOR).asMethod
        cm.reflectConstructor(ctor)
    }

    private def getFieldMirror[T](obj: T, fieldName: String): Any = {
        val m = universe.runtimeMirror(getClass.getClassLoader)
        val im = m.reflect(obj)
        val field = universe.typeOf[T].decl(universe.TermName(fieldName)).asTerm.accessed.asTerm
        im.reflectField(field).get
    }
}
