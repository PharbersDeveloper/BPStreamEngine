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
        return [ResultModel(
            data=event,
            tag=ResultTag.Error,
            errMsg=str(repr(traceback.format_exception(exType, exValue, exTrace)))
        )]

    # print("3. return data to data engine")


from py4j.java_gateway import JavaGateway, CallbackServerParameters, GatewayParameters

if __name__ == "__main__":

    try:
        py4j_port = sys.argv[1].decode("UTF-8", errors="ignore")

        gateway = JavaGateway(
            callback_server_parameters=CallbackServerParameters(),
            gateway_parameters=GatewayParameters(port=int(py4j_port)))

        while True:
            message = gateway.entry_point.pop()

            if message == "EMPTY":
                continue
            elif message == "EOF":
                gateway.entry_point.stopServer()
            else:
                for item in facade(message):
                    gateway.entry_point.writeHdfs(item.toJson())
    finally:
        os._exit(0)
