#!/usr/bin/env python3

import sys
sys.path.insert(0, "../..")
import ptest

tests = [
    #(ports, 'path'),
    (1, './ctrl-args.py'),
    (2, './ctrl-broker.py'),
    (2, './ctrl-dynsec.py'),
]

if __name__ == "__main__":
    test = ptest.PTest()
    test.run_tests(tests)
