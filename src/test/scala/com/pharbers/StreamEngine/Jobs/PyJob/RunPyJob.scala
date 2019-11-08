package com.pharbers.StreamEngine.Jobs.PyJob

import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer.BPSPythonJobContainer

object RunPyJob extends App {

    def directStart(): Unit = {
        val job = BPSPythonJobContainer(null, BPSparkSession())
        job.open()
        job.exec()
    }
    directStart()

    def c(): Unit = {

    }
}
