package com.pharbers.StreamEngine.Utils.Component.Node

import com.pharbers.StreamEngine.Utils.Config.BPSConfig
import com.pharbers.StreamEngine.Utils.Annotation.Component

object BaseNodeMsgHandler {
    private[Component] def apply(config: Map[String, String]): BaseNodeMsgHandler = new BaseNodeMsgHandler(config)
}

/** 功能描述
  *
  * @author dcs
  * @version 0.0
  * @since 2019/10/22 15:47
  * @note 一些值得注意的地方
  */
@Component(name = "BaseNodeMsgHandler", `type` = "NodeMsgHandler")
private[Component] class BaseNodeMsgHandler(config: Map[String, String]) extends NodeMsgHandler{
    val handlerConfig: BPSConfig = BPSConfig(configDef,  config)
    private var nodes: Map[String, NodeMsg] = Map.empty

    override def find(id: String): Option[NodeMsg] = nodes.get(id)

    override def add(node: NodeMsg): Unit = {
        if(nodes.contains(node.id)){
            throw new Exception(s"node already exists ${node.id}")
        } else {
            nodes = nodes ++ Map(node.id -> node)
            save(handlerConfig.getString(this.PATH_CONFIG_KEY))
        }
    }

    override def update(node: NodeMsg): Unit = {
        if(nodes.contains(node.id)){
            nodes = nodes ++ Map(node.id -> node)
            save(handlerConfig.getString(this.PATH_CONFIG_KEY))
        }else {
            throw new Exception("node不存在")
        }
    }

    override def remove(node: NodeMsg): Unit = {
        nodes = nodes -- List(node.id)
    }

    override def save(path: String): Unit = {
        //todo：save to hdfs
    }

    override def load(path: String): Unit = {
        //todo： load from hdfs
    }
}
