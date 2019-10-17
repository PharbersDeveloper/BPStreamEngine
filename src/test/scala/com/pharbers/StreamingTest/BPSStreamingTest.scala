package com.pharbers.StreamTest


import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
import org.scalatest.FunSuite

import scala.reflect.runtime.universe._

class BPSStreamingTest extends FunSuite {
    test("Streaming Partition test") {
        import scala.reflect.runtime.{universe => ru}
        val classMirror = ru.runtimeMirror(getClass.getClassLoader)         //获取运行时类镜像
//        val classTest = classMirror.reflect(new com.testclass)              //获取需要反射的类对象
        val methods = ru.typeOf[BPSparkSession]                                //构造获取方式的对象
        val a = methods.typeSymbol.annotations.head
//        val b = getCustomAnnotationData(a.tree)
//        def getCustomAnnotationData(tree: Tree) = {
//            val Apply(_, Literal(Constant(name: String)) :: Literal(Constant(itype: String)) :: Literal(Constant(factory: Option[String])) :: Nil) = tree
//            new Component(name, itype, factory)
//        }
        println("")
//        val method = methods.decl(ru.TermName(s"$函数名")).asMethod          //获取需要调用的函数
//        val result = classTest.reflectMethod(Method)(入参1, 入参2)           //反射调用函数,并传入入参

    }
}
