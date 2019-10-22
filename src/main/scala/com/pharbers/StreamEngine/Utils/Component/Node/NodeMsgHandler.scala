package com.pharbers.StreamEngine.Utils.Component.Node

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/10/22 15:11
  * @note 一些值得注意的地方
  */
trait NodeMsgHandler {
    final val PATH_CONFIG_KEY = "path"
    final val PATH_CONFIG_DOC = "保存路径"
    val configDef: ConfigDef = new ConfigDef()
            .define(PATH_CONFIG_KEY, Type.STRING, "/workData/streamingV2/nodeMsg", Importance.HIGH, PATH_CONFIG_DOC)

    def find(id: String): Option[NodeMsg]
    def add(node: NodeMsg)
    def update(node: NodeMsg)
    def remove(node: NodeMsg)
    def save(path: String)
    def load(path: String)
}

