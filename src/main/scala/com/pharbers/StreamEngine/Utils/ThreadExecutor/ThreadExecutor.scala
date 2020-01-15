package com.pharbers.StreamEngine.Utils.ThreadExecutor

import java.util.concurrent.{CountDownLatch, ExecutorService, Executors}

import com.pharbers.StreamEngine.Utils.Config.AppConfig

/** 功能描述
  *
  * @author dcs
  * @version 0.0
  * @since 2019/10/28 13:41
  * @note 一些值得注意的地方
  */

object ThreadExecutor {
    var executorService: Option[ExecutorService] = None
    val count = new CountDownLatch(1)
    def apply(): ExecutorService = {
        executorService match {
            case None => executorService = Some(Executors.newFixedThreadPool(AppConfig().getInt(AppConfig.THREAD_MAX_KEY)))
            case _ =>
        }
        executorService.get
    }

    def waitForShutdown(): Unit ={
        count.await()
    }

    def shutdown(): Unit ={
        count.countDown()
        executorService.get.shutdown()
    }

    def shutdownNow(): Unit ={
        count.countDown()
        executorService.get.shutdownNow()
    }
}
