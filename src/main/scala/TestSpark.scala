import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import org.apache.spark.sql.{Dataset, SparkSession}

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/11/27 11:27
  * @note 一些值得注意的地方
  */
object TestSpark {

    def main(args: Array[String]): Unit = {
        Run1.run()
        Run2.run()
        Run3.run()
        Run4.run()
        Run5.run()
        ThreadExecutor.waitForShutdown()
    }


    object Run1 extends Serializable {
        def run(): Unit = {
            val a = new TestClass
            a.init()
            val spark = BPSparkSession()
            import spark.implicits._
            val df = Seq("a", "b").toDS()
                    .map(x => {
                        x + a.a
                    })
            println("run1")
            df.show()
        }
    }

    object Run2 extends Serializable {
        val a = new TestClass
        a.init()
        def run(): Unit = {
            val spark = BPSparkSession()
            import spark.implicits._
            val df = Seq("a", "b").toDS()
                    .map(x => {
                        x + a.a
                    })
            println("run2")
            df.show()
        }
    }

    object Run3 extends Serializable {
        def run(): Unit = {
            TestObj.init()
            val spark = BPSparkSession()
            import spark.implicits._
            val df = Seq("a", "b").toDS()
                    .map(x => {
                        x + TestObj.a
                    })
            println("run3")
            df.show()
        }
    }

    object Run4 extends Serializable {
        def run(): Unit = {
            val a = TestObj
            a.init()
            val spark = BPSparkSession()
            import spark.implicits._
            val df = Seq("a", "b").toDS()
                    .map(x => {
                        x + a.a
                    })
            println("run4")
            df.show()
        }
    }

    object Run5 extends Serializable {
        val a = TestObj
        a.init()
        def run(): Unit = {
            val spark = BPSparkSession()
            import spark.implicits._
            val df = Seq("a", "b").toDS()
                    .map(x => {
                        x + a.a
                    })
            println("run5")
            df.show()
        }
    }

    class TestClass extends Serializable {
        println("初始化Test class")
        var a = "0"

        def init(): Unit = {
            println("改变test class a")
            a = "class"
        }
    }

    object TestObj extends Serializable {
        println("初始化TestObj")
        var a = "0"

        def init(): Unit = {
            println("改变test obj a")
            a = "obj"
        }
    }

}
