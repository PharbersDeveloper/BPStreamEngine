package com.pharbers.StreamEngine.Jobs.GenCubeJob.strategy

trait BPSStrategy[T] {

    def convert(data: T): T

}
