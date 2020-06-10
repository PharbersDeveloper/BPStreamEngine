package com.pharbers.StreamEngine

import com.pharbers.StreamEngine.BatchJobs.GenCubeJob.GenCubeJob
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor

object main_oom {
    def main(args: Array[String]): Unit = {
        BPSConcertEntry.start()
        ThreadExecutor.waitForShutdown()
    }
}

object main_gen_cube {
    def main(args: Array[String]): Unit = {

        GenCubeJob(sql = args(0), esIndex = args(1)).start()

    }
}
