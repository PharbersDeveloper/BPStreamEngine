package com.pharbers.macros.convert.jsonapi

import scala.reflect.macros.whitebox
import scala.language.experimental.macros
import com.pharbers.macros.api.JsonapiConvert

object JsonapiMacro {
    implicit def jsonapiMacroMaterialize[T]: JsonapiConvert[T] = macro impl[T]

    def impl[T](c: whitebox.Context)(ttag: c.WeakTypeTag[T]): c.Expr[JsonapiConvert[T]] = {
        import c.universe._

        val t_type = ttag.tpe

        val t_symbol = t_type match {
            case TypeRef(_, str, _) => str
        }
        val t_name = t_symbol.asClass.name.toString
        val t_type_name = TypeName(t_name)
        val tmp_class_name = TypeName(c.freshName("eval$"))

        val q"..$clsdef" = q"""{
        class $tmp_class_name extends JsonapiConvert[$t_type_name] {

            import com.pharbers.jsonapi.model._
            import com.pharbers.macros.convert.jsonapi._
            import com.pharbers.jsonapi.model.RootObject._
            import com.pharbers.macros.convert.jsonapi.ResourceObjectReader._

            override def fromJsonapi(jsonapi: RootObject): $t_type_name = {
                val jsonapi_data = jsonapi.data.get.asInstanceOf[ResourceObject]
                val included = jsonapi.included
                val entity = fromResourceObject[$t_type_name](jsonapi_data, included)(ResourceReaderMaterialize)
                entity
            }

            override def fromJsonapiLst(jsonapi: RootObject): List[$t_type_name] = {
                val jsonapi_data = jsonapi.data.get.asInstanceOf[ResourceObjects].array
                val included = jsonapi.included
                var entity: List[$t_type_name] = Nil
                jsonapi_data.foreach(x => {
                    entity = entity :+ fromResourceObject[$t_type_name](x, included)(ResourceReaderMaterialize)
                })
                entity
            }

            override def toJsonapi(obj: $t_type_name): RootObject = {
                val reo_includeds = toResourceObject(obj)

                RootObject(
                    data = Some(reo_includeds._1),
                    included = if(reo_includeds._2.resourceObjects.array.isEmpty) None else Some(reo_includeds._2)
                )
            }

            override def toJsonapi(objs: List[$t_type_name]): RootObject = {
                val data_included_lst = objs.map(toJsonapi)
                val dataLst = data_included_lst.map(_.data).filter(_.isDefined).map(_.get.asInstanceOf[ResourceObject])
                val includedLst = data_included_lst.map(_.included).filter(_.isDefined).flatMap(x => x.get.resourceObjects.array).distinct
                RootObject(
                    data = if(dataLst.isEmpty) None else Some(ResourceObjects(dataLst)),
                    included = if(includedLst.isEmpty) None else Some(Included(ResourceObjects(includedLst)))
                )
            }
        }
        }"""

        val reVal =q""" new $tmp_class_name """

        c.Expr[JsonapiConvert[T]](Block(clsdef.toList.asInstanceOf[List[c.universe.Tree]], reVal))
    }
}
