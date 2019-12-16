package com.pharbers.StreamEngine.Jobs.PyJob.Py4jServer

/** 管理 Py4j 中的一组 scala 的 server 端和 python 的 client
 *
 * @author clock
 * @version 0.1
 * @since 2019/12/05 10:07
 */
case class BPSPy4jManager() {
    // 保存当前 JVM 上运行的所有 Py4j 服务
    var servers: Map[String, BPSPy4jServer] = Map.empty

    def open(serverConf: Map[String, Any] = Map().empty): Unit = {
        this.synchronized {
            val server = BPSPy4jServer(serverConf)(pop, close).openBuffer().startServer().startEndpoint()
            servers = servers + (server.threadId -> server)
        }
    }

    def close(threadId: String): Unit = {
        this.synchronized {
            this.servers = this.servers - threadId
        }
    }

    // 保存流中的数据，并可以给 Python 访问
    var dataQueue: List[String] = Nil

    def push(message: String): Unit = {
        this.synchronized {
            this.dataQueue = this.dataQueue ::: message :: Nil
        }
    }

    def pop(): String = {
        this.synchronized {
            if (this.dataQueue.nonEmpty) {
                val result = this.dataQueue.head
                this.dataQueue = this.dataQueue.tail
                result
            } else "EMPTY"
        }
    }
}
