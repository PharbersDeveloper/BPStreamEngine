#!/usr/bin/python
# -*- coding: UTF-8 -*-

import sys
import json

reload(sys)
sys.setdefaultencoding('utf-8')

def main(argv):
    dict = json.loads(argv)
    dict["addTest"] = "哎呦"
    j = json.dumps(dict, ensure_ascii=False)
    return j

if __name__ == '__main__':
    try:
        data = sys.argv[1].decode('UTF-8', errors="ignore")
        print(main(data))
    except:
        exType, exValue, exTrace = sys.exc_info()
        print(exType, "\n", exValue, "\n", exTrace)
