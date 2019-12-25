package com.pharbers.StreamEngine.Jobs.Hive2EsJob.strategy

trait BPSStrategy[T] {

    def convert(data: T): T

}
