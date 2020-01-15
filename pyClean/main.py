#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import json

reload(sys)
sys.setdefaultencoding('utf-8')

from results import ResultTag
from results import ResultModel
from cleaning import process

def facade(message):
    # print("Pharbers Data Cleaning")

    # print("1. need check the context auth right")
    # pbs_auth_checking()

    # print("2. need continue with cleaning process ")
    try:
        # event = sys.argv[1].decode("UTF-8", errors="ignore")
        event = json.loads(message)
        return process(event)
    except:
        import traceback
        exType, exValue, exTrace = sys.exc_info()
        errMsg = {"exType": str(exType), "exValue": str(exValue), "exTrace": str(traceback.format_exc(exTrace))}
        return [ResultModel(
            data=event,
            tag=ResultTag.Error,
            errMsg=json.dumps(errMsg, ensure_ascii=False)
        )]

    # print("3. return data to data engine")


from py4j.protocol import Py4JNetworkError
from py4j.java_gateway import JavaGateway, CallbackServerParameters, GatewayParameters

if __name__ == "__main__":
    py4j_port = sys.argv[1].decode("UTF-8", errors="ignore")
    callback_port = sys.argv[2].decode("UTF-8", errors="ignore")

    gateway = JavaGateway(
        callback_server_parameters=CallbackServerParameters(port=int(callback_port)),
        gateway_parameters=GatewayParameters(port=int(py4j_port))
    )

    while True:
        try:
            message = gateway.entry_point.py4j_pop()
        except Py4JNetworkError:
            break

        if message == "EMPTY":
            continue
        elif message == "EOF":
            try:
                gateway.entry_point.py4j_stopServer()
            except Py4JNetworkError:
                break
        else:
            for item in facade(message):
                gateway.entry_point.py4j_writeHdfs(item.toJson())

    os._exit(0)
