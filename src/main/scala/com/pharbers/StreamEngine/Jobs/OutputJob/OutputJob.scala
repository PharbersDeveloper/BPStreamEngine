package com.pharbers.StreamEngine.Jobs.OutputJob

import org.apache.spark.sql.DataFrame

trait OutputJob {

    def sink(output: DataFrame): Unit = {}

}
