package com.pharbers.StreamEngine.Jobs.EsJob.strategy

trait BPSStrategy[T] {

    def convert(data: T): T

}
