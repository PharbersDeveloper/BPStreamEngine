package com.pharbers.StreamEngine

import com.pharbers.StreamEngine.BatchJobs.GenCubeJob.GenCubeJob
import com.pharbers.StreamEngine.BatchJobs.WriteToEsJob.WriteToEsJob
import com.pharbers.StreamEngine.BatchJobs.WriteToMongoJob.WriteToMongoJob
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
        GenCubeJob(jobId = args(0), sql = args(1)).start()
    }
}

object main_write_es {
    def main(args: Array[String]): Unit = {
        WriteToEsJob(jobId = args(0), target = args(1)).start()
    }
}

object main_write_mongo {
    def main(args: Array[String]): Unit = {
        WriteToMongoJob(jobId = args(0), uri = args(1), dbName = args(2), collName = args(3)).start()
    }
}
