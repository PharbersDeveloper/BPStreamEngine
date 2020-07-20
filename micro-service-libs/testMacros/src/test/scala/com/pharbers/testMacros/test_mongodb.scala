package com.pharbers.testMacros

import com.mongodb.DBObject
import com.pharbers.models.request._
import com.pharbers.util.log.phLogTrait
import com.pharbers.models.entity.{hospital, representative}
import com.pharbers.jsonapi.model.RootObject
import com.pharbers.jsonapi.json.circe.CirceJsonapiSupport
import com.pharbers.macros.convert.mongodb.{MongoMacro, TraitRequest}
import com.pharbers.mongodb.dbtrait.DBTrait

object test_mongodb extends App with CirceJsonapiSupport with phLogTrait {

    import com.pharbers.macros._
    import com.pharbers.macros.convert.jsonapi.JsonapiMacro._


    implicit val db: DBTrait[TraitRequest] = new MongoMacro[request]{}.queryDBInstance("client").get.asInstanceOf[DBTrait[TraitRequest]]

    def findOne: Option[AnyRef] = {
        val jsonData =
            """
              |{
              |    "data": {
              |        "type": "request",
              |        "id": "tmp_4a4e2a7f-0191-403e-8864-626320e963ca",
              |        "attributes": {
              |            "res": "dest"
              |        },
              |        "relationships": {
              |            "eqcond": {
              |                "data": [
              |                    {
              |                        "type": "eqcond",
              |                        "id": "tmp_00ed7bdc-eea2-437f-94b7-1fa18c447e05"
              |                    }
              |                ]
              |            }
              |        }
              |    },
              |    "included": [
              |        {
              |            "type": "eqcond",
              |            "id": "tmp_00ed7bdc-eea2-437f-94b7-1fa18c447e05",
              |            "attributes": {
              |                "val": "5b641342aa8de31ed8fb11e3",
              |                "key": "id"
              |            }
              |        }
              |    ]
              |}""".stripMargin
        val json_data = parseJson(jsonData)
        val jsonapi = decodeJson[RootObject](json_data)

        val requests = formJsonapi[request](jsonapi)
        println(requests)

        val result = queryObject[hospital](requests)
        println(result)
        println(result.get.id)
        result
    }
    //findOne

    def queryMulti: List[AnyRef] = {
        val jsonData =
            """
              |{
              |	"data": {
              |		"id": "1",
              |		"type": "request",
              |		"attributes": {
              |			"res": "test2"
              |		},
              |		"relationships": {
              |			"eq_conditions": {
              |				"data": [{
              |					"id": "2",
              |					"type": "eq_cond"
              |				}]
              |			},
              |         "fm_conditions": {
              |				"data": {
              |					"id": "3",
              |					"type": "fm_cond"
              |				}
              |			}
              |		}
              |	},
              |	"included": [{
              |		"id": "2",
              |		"type": "eq_cond",
              |		"attributes": {
              |			"key": "phone",
              |			"value": "18510971868"
              |		}
              |	},{
              |		"id": "3",
              |		"type": "fm_cond",
              |		"attributes": {
              |			"skip": 0,
              |			"take": 2
              |		}
              |	}]
              |}
            			""".stripMargin
        val json_data = parseJson(jsonData)
        val jsonapi = decodeJson[RootObject](json_data)

        val requests = formJsonapi[request](jsonapi)
        phLogTrait.phLog(requests)

        val result = queryMultipleObject[request](requests)
        println(result)
        result
    }
    //queryMulti

    def insert: DBObject = {
        val jsonData =
            """
              |{
              |	"data": {
              |		"id": "1",
              |		"type": "Consumers",
              |		"attributes": {
              |			"name": "Alex",
              |   		"age": 12,
              |     	"phone": "18510971868"
              |		},
              |		"relationships": {
              |			"orders": {
              |				"data": [{
              |					"id": "2",
              |					"type": "Order"
              |				}]
              |			}
              |		}
              |	},
              |	"included": [{
              |		"id": "2",
              |		"type": "Order",
              |		"attributes": {
              |			"title": "phone",
              |			"abc": 6400
              |		}
              |	}]
              |}
            			""".stripMargin
        val json_data = parseJson(jsonData)
        val jsonapi = decodeJson[RootObject](json_data)

        val consumers = formJsonapi[request](jsonapi)
        println(consumers)

        val result = insertObject[request](consumers)
//        consumers.orders.get.foreach { x =>
//            insertObject[Order](x)
//        }
//        println(result)
        result
    }
    //insert

    def updata: Int = {
        val jsonData =
            """
              |{
              |	"data": {
              |		"id": "1",
              |		"type": "request",
              |		"attributes": {
              |			"res": "Person"
              |		},
              |		"relationships": {
              |			"eqcond": {
              |				"data": [{
              |					"id": "2",
              |					"type": "eqcond"
              |				}]
              |			},
              |   		"upcond": {
              |     		"data": [{
              |					"id": "3",
              |					"type": "upcond"
              |				}]
              |     	}
              |		}
              |	},
              |	"included": [{
              |		"id": "2",
              |		"type": "eq_cond",
              |		"attributes": {
              |			"key": "name",
              |			"value": "Alex"
              |		}
              |	},{
              |		"id": "3",
              |		"type": "up_cond",
              |		"attributes": {
              |			"key": "name",
              |			"value": "Alex2"
              |		}
              |	}]
              |}
            			""".stripMargin
        val json_data = parseJson(jsonData)
        val jsonapi = decodeJson[RootObject](json_data)

        val requests = formJsonapi[request](jsonapi)
        println(requests)

        val result = updateObject[request](requests)
        println(result)
        result
    }
    //updata

    def delete = {
        val jsonData =
            """
              |{
              |	"data": {
              |		"id": "1",
              |		"type": "request",
              |		"attributes": {
              |			"res": "test"
              |		},
              |		"relationships": {
              |			"eq_conditions": {
              |				"data": [{
              |					"id": "2",
              |					"type": "eq_cond"
              |				}]
              |			}
              |		}
              |	},
              |	"included": [{
              |		"id": "2",
              |		"type": "eq_cond",
              |		"attributes": {
              |			"key": "phone",
              |			"value": "0000"
              |		}
              |	}]
              |}
            			""".stripMargin
        val json_data = parseJson(jsonData)
        val jsonapi = decodeJson[RootObject](json_data)

        val requests = formJsonapi[request](jsonapi)
        println(requests)

        val result = deleteObject(requests)
        phLog(result)
    }
    //delete

    def cond(): List[representative] = {
        val rq = new request
        rq.res = "representative"
        rq.eqcond = Some(eq2c("education", "本科") :: eq2c("rep_name", "小白") :: Nil)
        rq.ltcond = Some(lt2c("age", 31) :: Nil)
        rq.gtcond = Some(gt2c("age", 20) :: Nil)
        queryMultipleObject[representative](rq)
    }
    cond().foreach(println)

}
