package com.pharbers.StreamEngine.BatchJobs.GenCubeJob

import org.scalatest.FunSuite

class GenCubeJobTest extends FunSuite {

    test("GenCubeJob Test") {
        GenCubeJob(sql = "SELECT * FROM max_result").start
    }

}
