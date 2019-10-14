package com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy

trait BPSJobStrategy {
    def getTopic: String
    def getSchema: org.apache.spark.sql.types.DataType
}
