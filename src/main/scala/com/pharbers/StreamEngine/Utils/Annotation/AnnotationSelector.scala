package com.pharbers.StreamEngine.Utils.Annotation

import java.io.File
import java.lang.annotation.Annotation
import java.net.URL
import java.util
import java.util.jar.{JarEntry, JarFile}

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/10/16 14:51
  * @note 一些值得注意的地方
  */
object AnnotationSelector {
    /** 功能描述
      * 获取含有T注解的类路径和注解
      * @param packageName 包路径
      * @param tag 注解类的Class对象
      * @param childPackage 是否迭代搜索包
      * @tparam T 注解类型
      * @return scala.Seq[(_root_.scala.Predef.String, T)]
      * @author dcs
      * @version 0.0
      * @since 2019/9/17 10:21
      * @note 一些值得注意的地方
      * @example {{{这是一个例子}}}
      */
    def getAnnotationClass[T <: Annotation](packageName: String, tag: Class[T], childPackage: Boolean = false): Seq[(String, T)] = {
        val loader: ClassLoader = Thread.currentThread.getContextClassLoader
        val packagePath: String = packageName.replace(".", "/")
        val url: URL = loader.getResource(packagePath)
        if (url != null) {
            val classes = url.getProtocol match {
                case "file" => getClassNameByFile(url.getPath, childPackage, packagePath.replace("/", java.io.File.separator))
                case "jar" => getClassNameByJar(url.getPath, childPackage)
            }
            classes.map(x => {
                val classPath = x.substring(x.indexOf(packageName))
                (classPath, Class.forName(classPath).getAnnotation(tag))
            }).filter(x => x._2 != null)
        } else {
            Nil
        }
    }

    /**
      * 从项目文件获取某包下所有类
      *
      * @param filePath     文件路径
      * @param childPackage 是否遍历子包
      * @return 类完整路径
      */
    private def getClassNameByFile(filePath: String, childPackage: Boolean, packagePath: String): Seq[String] = {
        val separator = java.io.File.separator
        //todo: 在test里运行时因为会有同名包，这儿的url可能会变成test里面的出现bug,感觉这儿很容易出其他bug，\时window的分隔符，但是同时时java和四则的转义符
        val classPath = filePath.replace("test-classes/" + packagePath.replaceAll("\\\\", "/"), "classes/" + packagePath.replaceAll("\\\\", "/"))
        val file = new File(classPath)
        val childFiles = file.listFiles
        childFiles.filter(x => !x.isDirectory && x.getPath.endsWith(".class"))
                .map(x => x.getPath.substring(x.getPath.indexOf(packagePath), x.getPath.lastIndexOf(".")).replace(separator, ".")) ++
                childFiles.filter(x => x.isDirectory && childPackage).flatMap(x => getClassNameByFile(x.getPath, childPackage, packagePath))
    }

    private def getClassNameByJar(jarPath: String, childPackage: Boolean): Seq[String] = {
        import scala.collection.JavaConverters._
        val myClassName: util.List[String] = new util.ArrayList[String]
        val jarInfo: Array[String] = jarPath.split("!")
        val jarFilePath: String = jarInfo(0).substring(jarInfo(0).indexOf("/"))
        val packagePath: String = jarInfo(1).substring(1)
        try {
            val jarFile: JarFile = new JarFile(jarFilePath)
            val entrys: util.Enumeration[JarEntry] = jarFile.entries
            while ( {
                entrys.hasMoreElements
            }) {
                val jarEntry: JarEntry = entrys.nextElement
                var entryName: String = jarEntry.getName
                if (entryName.endsWith(".class")) if (childPackage) if (entryName.startsWith(packagePath)) {
                    entryName = entryName.replace("/", ".").substring(0, entryName.lastIndexOf("."))
                    myClassName.add(entryName)
                }
                else {
                    val index: Int = entryName.lastIndexOf("/")
                    var myPackagePath: String = null
                    if (index != -1) myPackagePath = entryName.substring(0, index)
                    else myPackagePath = entryName
                    if (myPackagePath == packagePath) {
                        entryName = entryName.replace("/", ".").substring(0, entryName.lastIndexOf("."))
                        myClassName.add(entryName)
                    }
                }
            }
        } catch {
            case e: Exception =>
                e.printStackTrace()
        }
        myClassName.asScala
    }
}
