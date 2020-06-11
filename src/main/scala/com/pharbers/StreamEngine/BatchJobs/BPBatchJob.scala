package com.pharbers.StreamEngine.BatchJobs

trait BPBatchJob {

    val id: String

    def start()

    def getJobStoragePath: String =
        "s3a://ph-stream/jobs" + s"/runId_$id" + s"/jobId_$id" + "/contents"

}
