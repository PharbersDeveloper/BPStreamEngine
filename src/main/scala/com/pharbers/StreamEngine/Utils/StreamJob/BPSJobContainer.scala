package com.pharbers.StreamEngine.Utils.StreamJob

trait BPSJobContainer extends BPStreamJob {
    var jobs: Map[String, BPStreamJob] = Map.empty
    def getJobWithId(id: String, category: String = ""): BPStreamJob = jobs(id)
    def finishJobWithId(id: String) = jobs -= id
}