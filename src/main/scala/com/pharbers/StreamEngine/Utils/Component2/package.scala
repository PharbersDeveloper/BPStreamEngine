package com.pharbers.StreamEngine.Utils

package object Component2 {
    trait BPConfig {
        val id: String
        val name: String
//        val factory: String
        val config: Map[String, String]
        val args: List[String]
        val strategies: Map[String, String] = str2map(config.get("strategies"))

        protected def str2map(str: Option[String]): Map[String, String]= {
            val sts = str match {
                case Some(str) => str.split(";").toList
                case None => Nil
            }
            sts.map (x => x.split(":")).map (x => x(0) -> x(1)).toMap
        }
    }

    case class BPStgConfig(
                              id: String,
                              name: String,
                              args: List[String],
                              config: Map[String, String]) extends BPConfig
    trait BPJobConfig extends BPConfig

    trait BPComponent {
        val config: BPConfig
    }
}
