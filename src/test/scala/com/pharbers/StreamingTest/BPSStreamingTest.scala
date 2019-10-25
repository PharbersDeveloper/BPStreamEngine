package com.pharbers.StreamTest


import java.net.{URL, URLClassLoader}

import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
import org.scalatest.FunSuite
import java.util.jar.JarFile
import collection.JavaConverters._
import scala.io.Source
import scala.reflect.runtime.universe._

class BPSStreamingTest extends FunSuite {
    test("Streaming Partition test") {
        import scala.reflect.runtime.{universe => ru}
        val classMirror = ru.runtimeMirror(getClass.getClassLoader) //获取运行时类镜像
        //        val classTest = classMirror.reflect(new com.testclass)              //获取需要反射的类对象
        val methods = ru.typeOf[BPSparkSession] //构造获取方式的对象
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

    test("scala 类加载") {
        println("begin")
//        val a = addJar("D:\\code\\pharbers\\BPStreamEngine\\target\\test-classes\\job-context.jar", "com.pharbers.ipaas.data.driver.libs.input.JsonInput")
        val a = getJsonInput("D:\\code\\pharbers\\BPStreamEngine\\target\\test-classes\\job-context.jar")
        println(a())
    }

    def getJsonInput(pathToJar: String): MethodMirror = {
        val jarFile = new JarFile(pathToJar)
        val e = jarFile.entries
        val urls = Array(new URL("jar:file:" + pathToJar + "!/"))
        val cl = URLClassLoader.newInstance(urls)

        val m = runtimeMirror(getClass.getClassLoader)
        val classSy = m.classSymbol(cl.loadClass("com.pharbers.ipaas.data.driver.libs.input.JsonInput"))
        val cm = m.reflectClass(classSy)
        val ctor = classSy.toType.decl(termNames.CONSTRUCTOR).asMethod
        cm.reflectConstructor(ctor)
    }

    def addJar(pathToJar: String, name: String): Unit ={
        val jarFile = new JarFile(pathToJar)
        val e = jarFile.entries

        val urls = Array(new URL("jar:file:" + pathToJar + "!/"))
        val cl = URLClassLoader.newInstance(urls)
        while ( {
            e.hasMoreElements
        }) {
            val je = e.nextElement
            if (je.isDirectory || !je.getName.endsWith(".class")) {

            } else {
                var className = je.getName.substring(0, je.getName.length - 6)
                className = className.replace('/', '.')
                try {
                    val c = cl.loadClass(className)
                } catch {
                    case e: Exception => e.printStackTrace()
                }
            }
        }
    }

}
