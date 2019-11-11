#!/usr/bin/python
# -*- coding: UTF-8 -*-

def create_mapping():
    return [
        {
            "ColName": "COMPANY",
            "ColDesc": "数据公司",
            "Type": "String",
            "Candidate": []
        },
        {
            "ColName": "SOURCE",
            "ColDesc": "数据来源",
            "Type": "String",
            "Candidate": []
        },
        {
            "ColName": "PROVINCE_NAME",
            "ColDesc": "省份名",
            "Type": "String",
            "Candidate": ["省", "省份", "省/自治区/直辖市", "PROVINCE", "Province", "PROVINCES"]
        },
        {
            "ColName": "CITY_NAME",
            "ColDesc": "城市名",
            "Type": "String",
            "Candidate": ["城市", "CITY", "City"]
        },
        {
            "ColName": "PREFECTURE_NAME",
            "ColDesc": "区县名",
            "Type": "String",
            "Candidate": ["区县"]
        },
        {
            "ColName": "YEAR",
            "ColDesc": "年份",
            "Type": "Integer",
            "Candidate": ["年", "YEARID", "YEAR", "Year"]
        },
        {
            "ColName": "QUARTER",
            "ColDesc": "季度",
            "Type": "String", #"Integer", TODO 月份里有个是2018Q1
            "Candidate": ["季度", "年季", "QUARTER"]
        },
        {
            "ColName": "MONTH",
            "ColDesc": "月份",
            "Type": "Integer",
            "Candidate": ["月", "YYYYMM", "年月", "PERIOD", "月份", "MONTH", "Month"]
        },
        {
            "ColName": "HOSP_NAME",
            "ColDesc": "医院名",
            "Type": "String",
            "Candidate": ["LILLY_HSPTL_NAME", "医院名称", "HOSPITAL NAME", "HOSPITAL", "Hospital"]
        },
        {
            "ColName": "HOSP_CODE",
            "ColDesc": "医院编码",
            "Type": "String",
            "Candidate": ["医院编码", "VEEVA_CUSTOMER_ID", "HOSP_ID", "Hospital Code", "CODE", "Code"]
        },
        {
            "ColName": "HOSP_LEVEL",
            "ColDesc": "医院等级",
            "Type": "String",
            "Candidate": ["医院等级", "HOSPITAL DEGREE", "LEVEL", "Level"]
        },
        {
            "ColName": "ATC",
            "ColDesc": "ATC编码",
            "Type": "String",
            "Candidate": ["ATC编码", "ATC码", "通用名ATC编码", "ATC", "ATC_CODE"]
        },
        {
            "ColName": "MOLE_NAME",
            "ColDesc": "分子名",
            "Type": "String",
            "Candidate": ["MOLECULE NAME", "MCL_NAME", "药品名", "分子名", "药品名称", "类别名称", "MOLECULE", "Molecule", "MOLE_NAME"]
        },
        {
            "ColName": "KEY_BRAND",
            "ColDesc": "通用名",
            "Type": "String",
            "Candidate": ["通用名"]
        },
        {
            "ColName": "PRODUCT_NAME",
            "ColDesc": "商品名",
            "Type": "String",
            "Candidate": ["商品名", "药品商品名", "PRODUCT", "Product", "PRODUCT_NAME"]
        },
        {
            "ColName": "PACK",
            "ColDesc": "包装",
            "Type": "String",
            "Candidate": ["包装", "包装单位", "PACK", "Pack", "PACKAGE"]
        },
        {
            "ColName": "SPEC",
            "ColDesc": "规格",
            "Type": "String",
            "Candidate": ["药品规格", "规格", "统一规格", "SPECIFICAT", "PACK_DES"]
        },
        {
            "ColName": "DOSAGE",
            "ColDesc": "剂型",
            "Type": "String",
            "Candidate": ["剂型", "FORM", "Form", "DOSAGE"]
        },
        {
            "ColName": "PACK_QTY",
            "ColDesc": "包装数量",
            "Type": "String",
            "Candidate": ["包装数量", "包装规格", "PACKAGE_QTY", "PACK_NUMBER", "PACKNUMBER"]
        },
        {
            "ColName": "SALES_QTY",
            "ColDesc": "销量",
            "Type": "Double",
            "Candidate": ["数量", "STANDARD_UNIT", "最小包装单位数量", "最小制剂单位数量", "QUANTITY", "Quantity"]
        },
        {
            "ColName": "SALES_VALUE",
            "ColDesc": "销售额",
            "Type": "Double",
            "Candidate": ["金额", "VALUE", "Value"]
        },
        {
            "ColName": "DELIVERY_WAY",
            "ColDesc": "给药途径",
            "Type": "String",
            "Candidate": ["给药途径", "ROAD", "途径", "ADMINST", "DELIVERY_WAY"]
        },
        {
            "ColName": "MANUFACTURER_NAME",
            "ColDesc": "生产厂商",
            "Type": "String",
            "Candidate": ["生产厂商", "CORP_NAME", "生产企业", "CORPORATIO", "Corporatio"],
        },
        {
            "ColName": "MKT",
            "ColDesc": "所属市场",
            "Type": "String",
            "Candidate": ["竞品市场", "MARKET", "Market"]
        }
    ]
