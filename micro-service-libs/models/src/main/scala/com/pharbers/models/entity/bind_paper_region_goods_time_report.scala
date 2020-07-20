package com.pharbers.models.entity

import com.pharbers.macros.api.commonEntity
import com.pharbers.macros.common.connecting.{One2OneConn, ToStringMacro}

//@One2OneConn[apm_sales_report]("apmreport")
@One2OneConn[apm_unit_report]("apmreport")
@ToStringMacro
class bind_paper_region_goods_time_report() extends commonEntity {
    var paper_id: String = ""
    var region_id: String = ""
    var goods_id: String = ""
    var time_type: String = ""
    var time: String = ""
    var report_id: String = ""
    var course_id: String = ""
}