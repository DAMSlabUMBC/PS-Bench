#!/usr/bin/env python3

import inspect
import os
import sys

sys.path.insert(0, "test")

import ptest
import test.apps.ctrl.test as p_ctrl
import test.apps.db_dump.test as p_db_dump
import test.apps.passwd.test as p_passwd
import test.apps.signal.test as p_signal
import test.broker.test as p_broker
import test.client.test as p_client
import test.lib.test as p_lib

test = ptest.PTest()
test.add_tests(p_client.tests, "test/client")
test.add_tests(p_lib.tests, "test/lib")
test.add_tests(p_ctrl.tests, "test/apps/ctrl")
test.add_tests(p_db_dump.tests, "test/apps/db_dump")
test.add_tests(p_passwd.tests, "test/apps/ctrl")
test.add_tests(p_signal.tests, "test/apps/signal")
test.add_tests(p_broker.tests, "test/broker")

test.run()
