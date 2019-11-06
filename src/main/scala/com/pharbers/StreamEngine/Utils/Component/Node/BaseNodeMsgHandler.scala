package com.pharbers.StreamEngine.Utils.Component.Node

import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Config.BPSConfig
import collection.JavaConverters._

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/10/22 15:47
  * @note 一些值得注意的地方
  */
@Component(name = "BaseNodeMsgHandler", `type` = "NodeMsgHandler")
private[Component] class BaseNodeMsgHandler(config: Map[String, String]) extends NodeMsgHandler{
    val handlerConfig: BPSConfig = new BPSConfig(configDef,  config.asJava)
    private var nodes: Map[String, NodeMsg] = Map.empty

    override def find(id: String): Option[NodeMsg] = nodes.get(id)

    override def add(node: NodeMsg): Unit = {
        if(nodes.contains(node.id)){
            throw new Exception("node already exists")
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

object BaseNodeMsgHandler {
    private[Component] def apply(config: Map[String, String]): BaseNodeMsgHandler = new BaseNodeMsgHandler(config)
}
