package com.pharbers.StreamEngine.Others.jeorch

import org.scalatest.FunSuite

class test extends FunSuite {

    test("companion and cartesian"){
        val dimensions =
            Map(
                "time" -> List("DATE", "QUARTER"), // QUARTER 是 result 原数据中没有的
                "geo" -> List("COUNTRY", "PROVINCE", "CITY"), // COUNTRY 是 result 原数据中没有的
                "prod" -> List("COMPANY", "MKT", "PRODUCT_NAME", "MOLE_NAME") // MKT 是 result 原数据中没有的
            )

//        val ks = dimensions.keySet
//        ks.subsets() foreach println
        initCuboids(dimensions) foreach println

        implicit class Crossable[X](xs: Traversable[X]) {
            def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
        }
        val c = dimensions("time") cross dimensions("geo") cross dimensions("prod")
        println(c)
        println(c.size)

        val l = List(dimensions("time"), dimensions("geo"), dimensions("prod"))
        val r = crossJoin(l)
        println(r)
        println(r.size)

    }

    def crossJoin[T](list: Traversable[Traversable[T]]): Traversable[Traversable[T]] =
        list match {
            case xs :: Nil => xs map (Traversable(_))
            case x :: xs => for {
                i <- x
                j <- crossJoin(xs)
            } yield Traversable(i) ++ j
        }

    def initCuboids(dimensions: Map[String, List[String]]): List[Map[String, List[String]]] = {
        var cuboids: List[Map[String, List[String]]] = List.empty
        for (s <- dimensions.keySet.subsets()) {
            var cuboid: Map[String, List[String]] = Map.empty
            for (k <- s) {
                cuboid += (k -> dimensions(k))
            }
            cuboids = cuboids :+ cuboid
        }
        cuboids
    }

    def genCartesianHierarchies(cuboid: Map[String, List[String]]) = {
        crossJoin(cuboid.values.toList)
    }

}
