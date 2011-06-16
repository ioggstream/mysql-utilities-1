#!/usr/bin/env python

import os
import diff
from mysql.utilities.exception import MySQLUtilError, MUTException

_ARGUMENTS = ['util_test.util_test', 'util_test.t3:util_test',
              'util_test:util_test.t3', 'util_test.t3.t3:util_test.t3',
              'util_test.t3:util_test..t4']

class test(diff.test):
    """check errors for diff
    This test executes of conditions to test the errors for the diff utility.
    It uses the diff test as a parent for setup and teardown methods.
    """

    def check_prerequisites(self):
        return diff.test.check_prerequisites(self)

    def setup(self):
        return diff.test.setup(self)

    def run(self):
        self.server1 = self.servers.get_server(0)
        self.res_fname = self.testdir + "result.txt"

        s1_conn = "--server1=" + self.build_connection_string(self.server1)
        s2_conn = "--server2=" + self.build_connection_string(self.server2)
       
        cmd_str = "mysqldiff.py %s %s util_test:util_test " % \
                  (s1_conn, s2_conn)

        test_num = 1
        cmd_opts = " --difftype=differ" 
        comment = "Test case %d - Use diff %s" % (test_num, cmd_opts)
        res = self.run_test_case(1, cmd_str + cmd_opts, comment)
        if not res:
            raise MUTException("%s: failed" % comment)

        cmd_str = "mysqldiff.py %s %s " % (s1_conn, s2_conn)

        test_num += 1
        cmd_opts = " util_test1:util_test" 
        comment = "Test case %d - database doesn't exist" % test_num
        res = self.run_test_case(1, cmd_str + cmd_opts, comment)
        if not res:
            raise MUTException("%s: failed" % comment)

        test_num += 1
        cmd_opts = " util_test:util_test2" 
        comment = "Test case %d - database doesn't exist" % test_num
        res = self.run_test_case(1, cmd_str + cmd_opts, comment)
        if not res:
            raise MUTException("%s: failed" % comment)

        test_num += 1
        cmd_opts = " util_test.t3:util_test.t33" 
        comment = "Test case %d - object doesn't exist" % test_num
        res = self.run_test_case(1, cmd_str + cmd_opts, comment)
        if not res:
            raise MUTException("%s: failed" % comment)

        test_num += 1
        cmd_opts = " util_test.t31:util_test.t3" 
        comment = "Test case %d - object doesn't exist" % test_num
        res = self.run_test_case(1, cmd_str + cmd_opts, comment)
        if not res:
            raise MUTException("%s: failed" % comment)

        for arg in _ARGUMENTS:
            test_num += 1
            cmd_opts = " %s" % arg 
            comment = "Test case %d - malformed argument%s " % \
                      (test_num, cmd_opts)
            res = self.run_test_case(2, cmd_str + cmd_opts, comment)
            if not res:
                raise MUTException("%s: failed" % comment)
                
        try:
            self.server1.exec_query("CREATE TABLE util_test.t5 (a int)")
            self.server2.exec_query("CREATE TABLE util_test.t6 (a int)")
        except:
            raise MUTException("Cannot create test tables.")
        
        test_num += 1
        cmd_opts = " util_test:util_test " 
        comment = "Test case %d - some objects don't exist in dbs" % test_num
        res = self.run_test_case(1, cmd_str + cmd_opts, comment)
        if not res:
            raise MUTException("%s: failed" % comment)

        return True

    def get_result(self):
        return self.compare(__name__, self.results)

    def record(self):
        return self.save_result_file(__name__, self.results)

    def cleanup(self):
        return diff.test.cleanup(self)