package com.pharbers.StreamEngine.Utils.Component.Dynamic

import java.util.Date
import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Component.Node.{NodeMsg, NodeMsgHandler}
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.StreamJob.BPDynamicStreamJob

/** 功能描述
  *
  * @author dcs
  * @version 0.0
  * @since 2019/10/23 13:28
  * @note 一些值得注意的地方
  */
@Component(name = "BaseDynamicStreamJobBuilder", `type` = "BPDynamicStreamJobBuilder")
private[Component] class BaseDynamicStreamJobBuilder(msgHandler: NodeMsgHandler, config: Map[String, String]) extends BPDynamicStreamJobBuilder {
    override def buildJob(jobMsg: JobMsg, job: BPDynamicStreamJob): BPDynamicStreamJob = {
        val func: (NodeMsg, JobMsg) => NodeMsg = (l, r) =>
            NodeMsg(l.id, l.`type`, l.classPath, l.args, l.listeners, l.handlers, l.jobs :+ (r.id, new Date().getTime), l.dependencies,
                l.dependencyArgs, l.config, l.dependencyStop, l.description, new Date().getTime)
        build(jobMsg, job)(func)
    }

    override def buildListener(jobMsg: JobMsg, listener: BPStreamListener): BPStreamListener = {
        val func: (NodeMsg, JobMsg) => NodeMsg = (l, r) =>
            NodeMsg(l.id, l.`type`, l.classPath, l.args, l.listeners :+ (r.id, new Date().getTime), l.handlers, l.jobs, l.dependencies,
                l.dependencyArgs, l.config, l.dependencyStop, l.description, new Date().getTime)
        build(jobMsg, listener)(func)
    }

    override def buildHandler(jobMsg: JobMsg, handler: BPSEventHandler): BPSEventHandler = {
        val func: (NodeMsg, JobMsg) => NodeMsg = (l, r) =>
            NodeMsg(l.id, l.`type`, l.classPath, l.args, l.listeners, l.handlers :+ (r.id, new Date().getTime), l.jobs, l.dependencies,
                l.dependencyArgs, l.config, l.dependencyStop, l.description, new Date().getTime)
        build(jobMsg, handler)(func)
    }

    private def build[T](jobMsg: JobMsg, job: T)(func: (NodeMsg, JobMsg) => NodeMsg): T = {
        msgHandler.add(NodeMsg(jobMsg.id, jobMsg.`type`, jobMsg.classPath, jobMsg.args, Nil, Nil, Nil, jobMsg.dependencies.map(x => (x, new Date().getTime)),
            jobMsg.dependencyArgs, jobMsg.config, jobMsg.dependencyStop, jobMsg.description, new Date().getTime))
        jobMsg.dependencies.foreach(x => {
            val dependencyJob = msgHandler.find(x).get
            msgHandler.update(func(dependencyJob, jobMsg))
        })
        job
    }
}

object BaseDynamicStreamJobBuilder {
    def apply(msgHandler: NodeMsgHandler, config: Map[String, String]): BaseDynamicStreamJobBuilder = new BaseDynamicStreamJobBuilder(msgHandler, config)
}
