#!/usr/bin/env python3

import os
import pathlib
import sys
sys.path.insert(0, "../..")
import ptest

tests = []

for test_file in pathlib.Path(os.path.abspath(os.path.dirname(__file__))).glob('passwd-*.py'):
    tests.append((1, test_file.resolve()))

if __name__ == "__main__":
    test = ptest.PTest()
    test.run_tests(tests)
