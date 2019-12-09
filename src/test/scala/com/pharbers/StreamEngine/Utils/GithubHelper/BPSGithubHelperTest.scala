package com.pharbers.StreamEngine.Utils.GithubHelper

import org.scalatest.FunSuite

class BPSGithubHelperTest extends FunSuite {
    val dir = "./test_github_helper_path/"
    val uri = "https://github.com/PharbersDeveloper/bp-data-clean.git"
    val branch = "v0.0.1"

    test("Test Clone By Branch") {
        val helper = BPSGithubHelper()
        helper.cloneByBranch(dir, uri, branch)
        assert(helper.listFile(dir, ".py") != Nil)
        helper.delDir(dir)
    }
}
