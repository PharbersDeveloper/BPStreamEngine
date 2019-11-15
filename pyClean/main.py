# #!/usr/bin/python
# # -*- coding: UTF-8 -*-
#
# import sys
# import json
#
# reload(sys)
# sys.setdefaultencoding('utf-8')
#
# from results import ResultTag
# from results import ResultModel
# from cleaning import process
#
# if __name__ == "__main__":
#     # print("Pharbers Data Cleaning")
#
#     # print("1. need check the context auth right")
#     # pbs_auth_checking()
#
#     # print("2. need continue with cleaning process ")
#     try:
#         # print('{"tag": 1, "data": {"ATC": "", "HOSP_NAME": "苏州市常熟市古里镇卫生院", "MKT": "", "SALES_VALUE": 0.0, "CITY_NAME": "苏州市", "PRODUCT_NAME": "施慧达", "KEY_BRAND": "", ", "SOURCE": "CPA", "DOSAGE": "素片", "MOLE_NAME": "苯磺酸左旋氨氯地平片", "YEAR": 2017, "QUARTER": "2017Q1", "PACK_QTY": "", "HOSP_CODE": "35738", "MANUFACTURER_NAME":  201701, "DELIVERY_WAY": "", "HOSP_LEVEL": "", "SALES_QTY": 0.0, "PREFECTURE_NAME": "常熟市", "PROVINCE_NAME": "江苏省", "SPEC": "", "PACK": "空"}, "errMsg": "", "metadajobId": "bf28d-1e0e-4abe-822c-bd09b0", "length": "_2084821_", "schema": [{"type": "String", "key": "COMPANY"}, {"type": "String", "key": "SOURCE"}, {"type": "String", "key": "PROVINCE_NAME"}, {"type": "String", "key": "CITY_NAME"}, {"type": "String", "key": "PREFECTURE_NAME"}, {"type": "Integer", "key": "YEAR"}, {"type": "String", "key": "QUARTER"}, {"type": "Integer", "key": "MONTH"}, {"type": "String", "key": "HOSP_NAME"}, {"type": "String", "key": "HOSP_CODE"}, {"type": "String", "key": "HOSP_LEVEL"}, {"type": "String", "key": "ATC"}, {"type": "String", "key": "MOLE_NAME"}, {"type": "String", "key": "KEY_BRAND"}, {"type": "String", "key": "PRODUCT_NAME"}, {"type": "String", "key": "PACK"}, {"type": "String", "key": "SPEC"}, {"type": "String", "key": "DOSAGE"}, {"type": "String", "key": "PACK_QTY"}, {"type": "Double", "key": "SALES_QTY"}, {"type": "Double", "key": "SALES_VALUE"}, {"type": "String", "key": "DELIVERY_WAY"}, {"type": "String", "key": "MANUFACTURER_NAME"}, {"type": "String", "key": "MKT"}], "traceId": "bf28d-1e0e-4abe-822c-bd09b", "fileName": "Pfizer_1701_1712_CPA.csv"}}')
#         event = sys.argv[1].decode("UTF-8", errors="ignore")
#         event = json.loads(event)
#         for item in process(event):
#             print(item.toJson())
#     except:
#         import traceback
#         exType, exValue, exTrace = sys.exc_info()
#         print(ResultModel(
#             data=event,
#             tag=ResultTag.Error,
#             errMsg=str(repr(traceback.format_exception(exType, exValue, exTrace)))
#         ).toJson())
#
#     # print("3. return data to data engine")

from py4j.java_gateway import JavaGateway, CallbackServerParameters

if __name__ == "__main__":
    gateway = JavaGateway(
        callback_server_parameters=CallbackServerParameters())
    gateway.entry_point.writeHdfs("abcdsdf")
    gateway.shutdown()
