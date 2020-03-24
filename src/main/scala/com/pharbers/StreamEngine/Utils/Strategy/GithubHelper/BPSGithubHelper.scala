package com.pharbers.StreamEngine.Utils.Strategy.GithubHelper

import java.io.File
import org.eclipse.jgit.api.Git

/** github 常用的操作接口
 *
 * @author clock
 * @version 0.1
 * @since 2019/12/09 16:48
 */
case class BPSGithubHelper() {

    def cloneByBranch(dir: String, uri: String, branch: String = "master"): Unit = {
        val file = new File(dir)

        if (file.exists()) pull() else clone()

        def clone(): Unit = Git.cloneRepository()
                .setDirectory(file)
                .setURI(uri)
                .setBranch(branch)
                .call()

        def pull(): Unit = Git.open(file).pull().call()
    }

    def listFile(dir: String, suffix: String = ""): List[String] = {
        var result: List[String] = Nil
        val file = new File(dir)
        if (file.isDirectory) {
            for (file <- file.listFiles()) {
                if(file.toString.endsWith(suffix))
                    result = result ::: file.toString :: Nil
            }
        }
        result
    }

    def delDir(dir: String): Unit = {
        val file = new File(dir)
        if (file.isDirectory) {
            for (file <- file.listFiles())
                if (file.isDirectory) delDir(file.toString)
                else file.delete()
            file.delete()
        } else file.delete()
    }
}
