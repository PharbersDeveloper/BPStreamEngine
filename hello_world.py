#!/usr/bin/python
# -*- coding: UTF-8 -*-

import sys
import json

def main(argv):
    dict = json.loads(argv)
    dict["addTest"] = "哎呦"
    j = json.dumps(dict, encoding="UTF-8", ensure_ascii=False)
    # print("%s " % "中文，sdfsafa,123141414 *@(#&@(^*$")
    print("%s " % j)

if __name__ == '__main__':
    main(sys.argv[1])
