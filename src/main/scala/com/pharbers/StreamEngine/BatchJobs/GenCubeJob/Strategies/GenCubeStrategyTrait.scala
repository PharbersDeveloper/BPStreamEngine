package com.pharbers.StreamEngine.BatchJobs.GenCubeJob.Strategies

import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.DataTypes

trait GenCubeStrategyTrait {

    //从维度层次组合中拆出时间维度层次
    def getFatherHierarchiesByDiKey(diKey: String, oneHierarchy: String, dimensions: Map[String, List[String]]): List[String] = {
        var result: List[String] = List.empty
        val hierarchies = dimensions(diKey)
        if (hierarchies.head == oneHierarchy) return result
        if (hierarchies.contains(oneHierarchy)) {
            for (i <- 0 until hierarchies.indexOf(oneHierarchy)) {
                result = result :+ hierarchies(i)
            }
        }
        result

    }

    //由于时间维度的三个层级YEAR/QUARTER/MONTH，在某一层级中只能求上期(lastYEAR/lastQUARTER/lastMONTH)的增长/增长率
    def lastTimeWindow(currentTimeHierarchy: String, currentWindow: WindowSpec): WindowSpec = {
        currentTimeHierarchy match {
            case "YEAR" => currentWindow
                .orderBy(col("YEAR").cast(DataTypes.IntegerType))
                .rangeBetween(-1, -1)
            case "QUARTER" => currentWindow
                .orderBy(col("YEAR").cast(DataTypes.IntegerType).*(100).+(col("QUARTER").cast(DataTypes.IntegerType).*(25)))
                .rangeBetween(-25, -25)
            case "MONTH" => currentWindow
                .orderBy(to_date(col("YEAR").cast(DataTypes.IntegerType).*(100).+(col("MONTH").cast(DataTypes.IntegerType)).cast(DataTypes.StringType), "yyyyMM").cast("timestamp").cast("long"))
                .rangeBetween(-86400 * 31, -86400 * 28)
        }
    }

    def currTimeWindow(currentTimeHierarchy: String, currentWindow: WindowSpec): WindowSpec = {
        currentTimeHierarchy match {
            case "YEAR" => currentWindow
                .orderBy(col("YEAR").cast(DataTypes.IntegerType))
                .rangeBetween(0, 0)
            case "QUARTER" => currentWindow
                .orderBy(col("YEAR").cast(DataTypes.IntegerType).*(100).+(col("QUARTER").cast(DataTypes.IntegerType).*(25)))
                .rangeBetween(0, 0)
            case "MONTH" => currentWindow
                .orderBy(to_date(col("YEAR").cast(DataTypes.IntegerType).*(100).+(col("MONTH").cast(DataTypes.IntegerType)).cast(DataTypes.StringType), "yyyyMM").cast("timestamp").cast("long"))
                .rangeBetween(0, 0)
        }
    }

}
