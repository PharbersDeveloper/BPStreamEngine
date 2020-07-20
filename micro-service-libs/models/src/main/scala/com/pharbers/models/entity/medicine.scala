package com.pharbers.models.entity

import com.pharbers.macros.api.commonEntity
import com.pharbers.macros.common.connecting.ToStringMacro

@ToStringMacro
class medicine() extends commonEntity {
    var form: String = ""
    var prod_category: String = ""
    var corp_name: String = ""
    var prod_name: String = ""
    var prod_image: String = ""
    var launch_time: Int = 0
    var insure_type: String = ""
    var research_type: String = ""
    var ref_price: Double = 0L
    var unit_cost: Double = 0L
    var therapeutic_field: String = ""
    var prod_features: String = ""
    var target_department: String = ""
    var prod_describe: String = ""
}