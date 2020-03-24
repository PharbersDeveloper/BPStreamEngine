package com.pharbers.StreamEngine.Utils.Strategy.GithubHelper

import java.io.File

import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Strategy.BPStrategyComponent
import org.apache.kafka.common.config.ConfigDef
import org.eclipse.jgit.api.Git

/** github 常用的操作接口
 *
 * @author clock
 * @version 0.1
 * @since 2019/12/09 16:48
 */
object BPSGithubHelper {
    def apply(componentProperty: Component2.BPComponentConfig): BPSGithubHelper =
        new BPSGithubHelper(componentProperty)
}

@Component(name = "BPSGithubHelper", `type` = "BPSGithubHelper")
class BPSGithubHelper(override val componentProperty: Component2.BPComponentConfig)
    extends BPStrategyComponent {

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

    override def createConfigDef(): ConfigDef = new ConfigDef()
    override val strategyName: String = "git repo"
}
