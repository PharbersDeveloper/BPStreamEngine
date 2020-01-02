#!/usr/bin/python
# -*- coding: UTF-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf8')

def cpa_gyc_mapping():
    return [
        {
            "ColName": "COMPANY",
            "ColDesc": "数据公司",
            "Type": "String",
            "Candidate": [],
            "NotNull": True
        },
        {
            "ColName": "SOURCE",
            "ColDesc": "数据来源",
            "Type": "String",
            "Candidate": ["数据来源"],
            "NotNull": True
        },
        {
            "ColName": "PROVINCE_NAME",
            "ColDesc": "省份名",
            "Type": "String",
            "Candidate": ["省", "省份", "省/自治区/直辖市", "PROVINCE", "PROVINCES",
                          "PROVINCE_NAME"],
        },
        {
            "ColName": "CITY_NAME",
            "ColDesc": "城市名",
            "Type": "String",
            "Candidate": ["城市", "CITY", "CITY_NAME"],
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
            "Candidate": ["年", "年月", "YEARID", "YEAR", "PERIOD", "YEAR_MONTH", "DATE"],
            "NotNull": True
        },
        {
            "ColName": "QUARTER",
            "ColDesc": "季度",
            "Type": "String",  # "Integer", TODO 月份里有个是2018Q1
            "Candidate": ["季度", "年季", "QUARTER", "季.度"]
        },
        {
            "ColName": "MONTH",
            "ColDesc": "月份",
            "Type": "String",
            "Candidate": ["月", "YYYYMM", "月份", "MONTH", "YEAR_MONTH"]
        },
        {
            "ColName": "HOSP_NAME",
            "ColDesc": "医院名",
            "Type": "String",
            "Candidate": ["LILLY_HSPTL_NAME", "医院名称", "HOSPITAL NAME", "HOSPITAL",
                          "HOSPITAL.NAME", "HOSNAME", "HOSPITAL_NAME", "医院"],
            # "NotNull": True
        },
        {
            "ColName": "HOSP_CODE",
            "ColDesc": "医院编码",
            "Type": "String",
            "Candidate": ["医院编码", "VEEVA_CUSTOMER_ID", "HOSP_ID", "HOSPITAL CODE", "CODE",
                          "HOSPITAL.CODE", "HOSPITAL_CODE", "医院.代码", "UCBHOSCODE",
                          "HOSCODE", "医院.编码", "ID", "DSCN医院编码"]
        },
        {
            "ColName": "HOSP_LEVEL",
            "ColDesc": "医院等级",
            "Type": "String",
            "Candidate": ["医院等级", "HOSPITAL DEGREE", "LEVEL", "级别", "等级", "医院级别"]
        },
        {
            "ColName": "ATC",
            "ColDesc": "ATC编码",
            "Type": "String",
            "Candidate": ["ATC编码", "ATC码", "通用名ATC编码", "ATC", "ATC_CODE", "ATC3编码",
                          "ATC3_CODE", "ATC3名称"]
        },
        {
            "ColName": "MOLE_NAME",
            "ColDesc": "分子名",
            "Type": "String",
            "Candidate": ["MOLECULENAME", "MOLECULE NAME", "MOLECULE_NAME", "MCL_NAME", "药品名",
                          "分子名", "药品名称", "类别名称", "MOLECULE", "MOLECULE",
                          "MOLE_NAME", "MOLECULE.NAME", "化学名", "通用名", "KEY_BRAND"],
            "NotNull": True
        },
        {
            "ColName": "KEY_BRAND",
            "ColDesc": "通用名",
            "Type": "String",
            "Candidate": []
        },
        {
            "ColName": "PRODUCT_NAME",
            "ColDesc": "商品名",
            "Type": "String",
            "Candidate": ["商品名", "药品商品名", "PRODUCT", "PRODUCT_NAME",
                          "PRODUCT.NAME", "PRODUCTNAME", "商品名称"],
            "NotNull": True
        },
        {
            "ColName": "PACK",
            "ColDesc": "包装",
            "Type": "String",
            "Candidate": ["包装", "包装单位", "PACKAGE", "包装.单位", "包.装"],
            # "NotNull": True
        },
        {
            "ColName": "SPEC",
            "ColDesc": "规格",
            "Type": "String",
            "Candidate": ["药品规格", "包装规格", "规格", "统一规格", "SPECIFICAT", "PACK_DES", "品规",
                          "PACK_DESCRIPTION", "PACK", "SKU"],
            "NotNull": True # TODO: 需要决策树
        },
        {
            "ColName": "DOSAGE",
            "ColDesc": "剂型",
            "Type": "String",
            "Candidate": ["剂型", "FORM", "DOSAGE", "FORMULATION_NAME", "CONTENT_TYPE", "CONTENTTYPE", "APP2_COD"],
            "NotNull": True
        },
        {
            "ColName": "PACK_QTY",
            "ColDesc": "包装数量",
            "Type": "String",
            "Candidate": ["包装数量", "PACKAGE_QTY", "PACK_NUMBER", "PACKNUMBER", "数量.（支/片）",
                          "数量（支/片）", "数量支/片", "数量(支/片)", "数量.(支/片)",
                          "数量.支/片", "包装.数量", "QUANTITY", "SIZE"],
            # "NotNull": True
        },
        {
            "ColName": "SALES_QTY",
            "ColDesc": "销量",
            "Type": "Double",
            "Candidate": ["数量", "STANDARD_UNIT", "最小包装单位数量", "最小制剂单位数量", "QUANTITY",
                          "销售数量", "TOTAL_UNITS", "UNIT", "SALES_QTY"],
            # "NotNull": True
        },
        {
            "ColName": "SALES_VALUE",
            "ColDesc": "销售额",
            "Type": "Double",
            "Candidate": ["金额", "金额元","金额(元)", "金额（元）", "金额.元", "销售金AM", "VALUE",
                          "SALES VALUE (RMB)", "SALES VALUERMB", "SALESVALUERMB", "SALES_VALUE",
                          "销售金额", "金额.（元）", "金额.(元)"],
            "NotNull": True
        },
        {
            "ColName": "DELIVERY_WAY",
            "ColDesc": "给药途径",
            "Type": "String",
            "Candidate": ["给药途径", "ROAD", "途径", "ADMINST", "DELIVERY_WAY", "给药.途径"]
        },
        {
            "ColName": "MANUFACTURER_NAME",
            "ColDesc": "生产厂商",
            "Type": "String",
            "Candidate": ["生产厂商", "生产厂家", "企业名称", "CORP_NAME", "生产企业",
                          "CORPORATIO", "MANUFACTUER_NAME", "CORPORATION",
                          "药厂名称", "集团名称", "COMPANY_NAME", "集团"],
            "NotNull": True
        },
        {
            "ColName": "MKT",
            "ColDesc": "所属市场",
            "Type": "String",
            "Candidate": ["竞品市场", "MARKET", "定义市场", "市场定义", "市场"],
            # "NotNull": True
        }
    ]


def chc_mapping():
    return [
        {
            "ColName": "COMPANY",
            "ColDesc": "数据公司",
            "Type": "String",
            "Candidate": [],
            "NotNull": True
        },
        {
            "ColName": "SOURCE",
            "ColDesc": "数据来源",
            "Type": "String",
            "Candidate": ["数据来源"],
            "NotNull": True
        },
        {
            "ColName": "YEAR",
            "ColDesc": "年份",
            "Type": "Integer",
            "Candidate": ["年", "YEARID", "YEAR"],
            "NotNull": True
        },
        {
            "ColName": "QUARTER",
            "ColDesc": "季度",
            "Type": "String",  # "Integer", TODO 月份里有个是2018Q1
            "Candidate": ["季度", "年季", "QUARTER", "季.度"]
        },
        {
            "ColName": "MONTH",
            "ColDesc": "月份",
            "Type": "Integer",
            "Candidate": ["月", "YYYYMM", "年月", "PERIOD", "月份", "MONTH", "YEAR_MONTH"]
        },
        {
            "ColName": "PROVINCE_NAME",
            "ColDesc": "省份名",
            "Type": "String",
            "Candidate": ["省", "省份", "省/自治区/直辖市", "PROVINCE", "PROVINCES",
                          "PROVINCE_NAME"],
        },
        {
            "ColName": "CITY_NAME",
            "ColDesc": "城市名",
            "Type": "String",
            "Candidate": ["城市", "CITY", "CITY_NAME"],
        },
        {
            "ColName": "DISTRICT",
            "ColDesc": "区县名",
            "Type": "String",
            "Candidate": ["区县"]
        },
        {
            "ColName": "HOSP_NAME",
            "ColDesc": "医院名称",
            "Type": "String",
            "Candidate": ["医院名称"],
            "NotNull": True
        },
        {
            "ColName": "HOSP_CODE",
            "ColDesc": "医院编码",
            "Type": "String",
            "Candidate": ["医院编码", "VEEVA_CUSTOMER_ID", "HOSP_ID", "HOSPITAL CODE", "CODE",
                          "HOSPITAL.CODE", "HOSPITAL_CODE", "医院.代码", "UCBHOSCODE",
                          "HOSCODE", "医院.编码", "ID", "DSCN医院编码"]
        },
        {
            "ColName": "HOSP_LEVEL",
            "ColDesc": "医院等级",
            "Type": "String",
            "Candidate": ["医院等级", "HOSPITAL DEGREE", "LEVEL", "级别", "等级", "医院级别"]
        },
        {
            "ColName": "HOSP_TYPE",
            "ColDesc": "医院类型",
            "Type": "String",
            "Candidate": ["医院类型"]
        },
        {
            "ColName": "HOSP_LIBRARY_TYPE",
            "ColDesc": "医院库类型",
            "Type": "String",
            "Candidate": ["医院库类型"]
        },
        {
            "ColName": "HOSP_REGION_TYPE",
            "ColDesc": "医院区域类型",
            "Type": "String",
            "Candidate": ["医院区域类型"]
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
            "Candidate": ["商品名", "药品商品名", "PRODUCT", "PRODUCT_NAME",
                          "PRODUCT.NAME", "PRODUCTNAME", "商品名称"],
            "NotNull": True
        },
        {
            "ColName": "SPEC",
            "ColDesc": "规格",
            "Type": "String",
            "Candidate": ["药品规格", "包装规格", "规格", "统一规格", "SPECIFICAT", "PACK_DES", "品规",
                          "PACK_DESCRIPTION"],
            # "NotNull": True
        },
        {
            "ColName": "DOSAGE",
            "ColDesc": "剂型",
            "Type": "String",
            "Candidate": ["剂型", "FORM", "DOSAGE", "FORMULATION_NAME"],
            # "NotNull": True
        },
        {
            "ColName": "PACK_NUMBER",
            "ColDesc": "价格转换比",
            "Type": "String",
            "Candidate": ["价格转换比"]
        },
        {
            "ColName": "PACK_UNIT",
            "ColDesc": "包装单位",
            "Type": "String",
            "Candidate": ["包装单位"]
        },
        {
            "ColName": "MANUFACTURER_NAME",
            "ColDesc": "生产厂商",
            "Type": "String",
            "Candidate": ["生产厂商", "生产厂家", "企业名称", "CORP_NAME", "生产企业",
                          "CORPORATIO", "MANUFACTUER_NAME", "CORPORATION",
                          "药厂名称", "集团名称", "COMPANY_NAME", "集团"],
            "NotNull": True
        },
        {
            "ColName": "PRICE",
            "ColDesc": "价格",
            "Type": "Double",
            "Candidate": ["价格"],
            "NotNull": True
        },
        {
            "ColName": "VOLUME",
            "ColDesc": "数量",
            "Type": "Double",
            "Candidate": ["数量"],
            "NotNull": True
        },
        {
            "ColName": "VALUE",
            "ColDesc": "金额",
            "Type": "Double",
            "Candidate": ["金额"],
            "NotNull": True
        },
        {
            "ColName": "PIECE",
            "ColDesc": "最小使用单位数量",
            "Type": "Double",
            "Candidate": ["最小使用单位数量"]
        }
    ]


def result_mapping():
    return [
        {
            "ColName": "DATE",
            "ColDesc": "结果数据年月",
            "Type": "String",
            "Candidate": ["月份", "年月", "DATE", "YEARMONTH"],
            "NotNull": True,
        },
        {
            "ColName": "PROVINCE",
            "ColDesc": "结果数据省份（对应医院大全）",
            "Type": "String",
            "Candidate": ["PHARBER.PROVINCE", "省份", "PROVINCE"],
            "NotNull": True,
        },
        {
            "ColName": "CITY",
            "ColDesc": "结果数据城市（对应医院大全）",
            "Type": "String",
            "Candidate": ["PHARBER.CITY", "城市", "CITY"],
            "NotNull": True,
        },
        {
            "ColName": "PHAID",
            "ColDesc": "结果数据中有两重聚合，有的是城市级别，则这个为空另一种为医院级别，则这个为PHA_ID",
            "Type": "String",
            "Candidate": ["HOSP_ID", "PANEL_ID", "医院PHACODE"],
        },
        {
            "ColName": "HOSP_NAME",
            "ColDesc": "医院的名称",
            "Type": "String",
            "Candidate": ["HOSP_NAME"],
        },
        {
            "ColName": "CPAID",
            "ColDesc": "同上，表面CPA&GYC ID",
            "Type": "String",
            "Candidate": ["ID"],
        },
        {
            "ColName": "PRODUCT_NAME",
            "ColDesc": "结果的产品名",
            "Type": "String",
            "Candidate": ["PRODUCT_NAME", "商品名", "PRODUCT", "PROD_NAME", "商品名_标准"],
            "NotNull": True,
        },
        {
            "ColName": "MOLE_NAME",
            "ColDesc": "结果的通用名",
            "Type": "String",
            "Candidate": ["通用名", "MOLECULE", "MOLECULE NAME", "MOLECULE_CN"],
        },
        {
            "ColName": "DOSAGE",
            "ColDesc": "结果数据剂型",
            "Type": "String",
            "Candidate": ["DOSAGE", "FORM", "剂型", "FORMULATION"],
        },
        {
            "ColName": "SPEC",
            "ColDesc": "规格",
            "Type": "String",
            "Candidate": ["SPECIFICATION", "规格", "SPECIFICATIONS"],
        },
        {
            "ColName": "PACK_QTY",
            "ColDesc": "包装数量",
            "Type": "String",
            "Candidate": ["包装数量", "包装数", "PACKAGENO", "SIZE", "最小包装数量"],
        },
        {
            "ColName": "SALES_VALUE",
            "ColDesc": "销售额",
            "Type": "Double",
            "Candidate": ["金额", "销售金额", "MTH_SALES_VALUE", "PREDICTED_SALES", "SALES",
                          "金额（元）", "金额(元)", "金额.元", "金额元"],
        },
        {
            "ColName": "SALES_QTY",
            "ColDesc": "销售量",
            "Type": "Double",
            "Candidate": ["销售数量", "数量", "数量（片）", "数量片", "数量.片", "UNITS", "MTH_SALES_VOLUME",
                          "PREDICTED_UNITS", "销售数量（最小单位:片支）"],
        },
        {
            "ColName": "F_SALES_VALUE",
            "ColDesc": "销售额",
            "Type": "Double",
            "Candidate": ["F_SALES"],
        },
        {
            "ColName": "F_SALES_QTY",
            "ColDesc": "销售量",
            "Type": "Double",
            "Candidate": ["F_UNITS"],
        },
        {
            "ColName": "MANUFACTURE_NAME",
            "ColDesc": "生产厂家",
            "Type": "String",
            "Candidate": ["生产企业", "生产厂家", "BELONG2COMPANY", "MANUFACTURER", "CORPORATION", "MANUFACTURE"],
        },
    ]
