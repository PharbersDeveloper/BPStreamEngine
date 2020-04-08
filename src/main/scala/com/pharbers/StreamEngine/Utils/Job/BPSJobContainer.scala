package com.pharbers.StreamEngine.Utils.Job

trait BPSJobContainer extends BPStreamJob {
    var jobs: Map[String, BPStreamJob] = Map.empty
    //todo: 这儿作为JobContainer接口的api，调用者很难知道category能够传入些什么,而且作为默认实现这儿会抛出keyNotFound异常
    def getJobWithId(id: String, category: String = ""): BPStreamJob = jobs(id)
    def finishJobWithId(id: String) = jobs -= id
}
