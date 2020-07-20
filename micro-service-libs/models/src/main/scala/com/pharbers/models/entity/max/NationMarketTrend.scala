package com.pharbers.models.entity.max
import com.pharbers.macros.api.commonEntity
import com.pharbers.macros.common.connecting.{One2ManyConn, One2OneConn, ToStringMacro}

@ToStringMacro
@One2OneConn[ProdSalesOverview]("ProdSalesOverview")
@One2ManyConn[MultipleLine]("MultipleLine")
class NationMarketTrend extends commonEntity {
    var title = "nationMarketTrend"
}
