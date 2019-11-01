#!/usr/bin/python
# -*- coding: UTF-8 -*-

def create_mapping():
    return [
        {
            "ColName": "PROVINCE_NAME", "Type": "String",
            "Candidate": ["省", "省份", "省/自治区/直辖市", "PROVINCE"]
        },
        {
            "ColName": "CITY_NAME", "Type": "String",
            "Candidate": ["城市", "CITY"]
        },
        {
            "ColName": "YEAR", "Type": "Integer",
            "Candidate": ["年", "YEARID", "YEAR"]
        },
        {
            "ColName": "QUARTER", "Type": "Integer",
            "Candidate": ["季度", "年季", "QUARTER"]
        },
        {
            "ColName": "MONTH", "Type": "Integer",
            "Candidate": ["月", "YYYYMM", "年月", "PERIOD", "月份", "MONTH"]
        },
        {
            "ColName": "HOSP_NAME", "Type": "String",
            "Candidate": ["LILLY_HSPTL_NAME", "医院名称", "HOSPITAL NAME", "HOSPITAL"]
        },
        {
            "ColName": "HOSP_CODE", "Type": "String",
            "Candidate": ["医院编码", "VEEVA_CUSTOMER_ID", "HOSP_ID", "Hospital Code", "CODE"]
        },
        {
            "ColName": "HOSP_LEVEL", "Type": "String",
            "Candidate": ["医院等级", "HOSPITAL DEGREE", "LEVEL"]
        },
        {
            "ColName": "ATC", "Type": "String",
            "Candidate": ["ATC编码", "ATC码", "通用名ATC编码", "ATC"]
        },
        {
            "ColName": "MOLE_NAME", "Type": "String",
            "Candidate": ["MOLECULE NAME", "MCL_NAME", "药品名", "分子名", "药品名称", "类别名称", "MOLECULE"]
        },
        {
            "ColName": "KEY_BRAND", "Type": "String",
            "Candidate": ["通用名"]
        },
        {
            "ColName": "PRODUCT_NAME", "Type": "String",
            "Candidate": ["商品名", "药品商品名", "PRODUCT"]
        },
        {
            "ColName": "PACK", "Type": "String",
            "Candidate": ["包装", "包装单位", "PACK_DES", "PACK"]
        },
        {
            "ColName": "SPEC", "Type": "String",
            "Candidate": ["药品规格", "规格", "统一规格", "SPECIFICAT"]
        },
        {
            "ColName": "PACK_QTY", "Type": "String",
            "Candidate": ["包装数量", "包装规格", "PACKAGE_QTY", "PACK_NUMBER", "PACKNUMBER"]
        },
        {
            "ColName": "SALES_VALUE", "Type": "Double",
            "Candidate": ["金额", "VALUE"]
        },
        {
            "ColName": "SALES_QTY", "Type": "Double",
            "Candidate": ["数量", "STANDARD_UNIT", "最小包装单位数量", "最小制剂单位数量", "QUANTITY"]
        },
        {
            "ColName": "DOSAGE", "Type": "String",
            "Candidate": ["剂型", "FORM"]
        },
        {
            "ColName": "DELIVERY_WAY", "Type": "String",
            "Candidate": ["给药途径", "ROAD", "途径", "ADMINST"]
        },
        {
            "ColName": "MANUFACTURER_NAME", "Type": "String",
            "Candidate": ["生产厂商", "CORP_NAME", "生产企业", "CORPORATIO"],
        },
        {
            "ColName": "MKT", "Type": "String",
            "Candidate": ["竞品市场", "MARKET"]
        }
    ]

