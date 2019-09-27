package com.pharbers.StreamEngine.BPStreamJob.JobStrategy

trait JobStrategy {
    def getTopic: String
    def getSchema: org.apache.spark.sql.types.DataType
}
