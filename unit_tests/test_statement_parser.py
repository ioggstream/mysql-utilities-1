import unittest

from mysql.utilities.command.dbexport import create_preliminary_view


def compare_lower(a, b):
    assert a.lower().strip() == b.lower().strip(), "%r vs %r" % (a, b)


class ViewParserTest(unittest.TestCase):
    def test_preliminary_view_basic(self):
        ret = create_preliminary_view(
                "CREATE VIEW myview AS select `t`.`a` AS `a`, `t`.`b` AS `b` FROM t;\n")
        print(ret)
        compare_lower(ret, "CREATE VIEW myview AS SELECT 1  AS `a`;\n")

    def test_preliminary_view_union(self):
        # SELECT UNION uses only column names from the first view.
        ret = create_preliminary_view(
                "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` SQL SECURITY DEFINER "
                "VIEW `logins` AS"
                " select `logs`.`utente_login`.`quando` AS `quando`,"
                "`logs`.`utente_login`.`sessione` AS `sessione`,"
                "`logs`.`utente_login`.`ip` AS `ip`,"
                "`logs`.`utente_login`.`server_name` AS `server_name`,"
                "`logs`.`utente_login`.`user_agent` AS `user_agent`,"
                "`cms`.`loggabile`.`login` AS `login`,"
                "'OK' AS `esito` "
                "from (`logs`.`utente_login` join `cms`.`loggabile` on"
                "((`logs`.`utente_login`.`id_utente` = `cms`.`loggabile`.`id_utente`))) "
                "union select `logs`.`login_falliti`.`quando` AS `quando`,"
                "`logs`.`login_falliti`.`sessione` AS `sessione`,"
                "`logs`.`login_falliti`.`ip` AS `ip`,"
                "`logs`.`login_falliti`.`server_name` AS `server_name`,"
                "`logs`.`login_falliti`.`user_agent` AS `user_agent`,"
                "`logs`.`login_falliti`.`login` AS `login`,"
                "'KO' AS `KO` from `logs`.`login_falliti`;\n ")
        print(ret)
        compare_lower(ret, "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` SQL SECURITY DEFINER "
                           "VIEW `logins` AS "
                           "SELECT 1  AS `quando`,"
                           "1  AS `sessione`,"
                           "1  AS `ip`,"
                           "1  AS `server_name`,"
                           "1  AS `user_agent`,"
                           "1  AS `login`;\n")

    def test_preliminary_view_sys(self):
        ret = create_preliminary_view(
                "CREATE ALGORITHM=TEMPTABLE DEFINER=`mysql.sys`@`localhost` SQL SECURITY INVOKER "
                "VIEW `x$ps_schema_table_statistics_io` AS"
                " select `extract_schema_from_file_name`(`performance_schema`.`file_summary_by_instance`.`FILE_NAME`) AS `table_schema`,"
                "`extract_table_from_file_name`(`performance_schema`.`file_summary_by_instance`.`FILE_NAME`) AS `table_name`,"
                "sum(`performance_schema`.`file_summary_by_instance`.`COUNT_READ`) AS `count_read`,"
                "sum(`performance_schema`.`file_summary_by_instance`.`SUM_NUMBER_OF_BYTES_READ`) AS `sum_number_of_bytes_read`,"
                "sum(`performance_schema`.`file_summary_by_instance`.`SUM_TIMER_READ`) AS `sum_timer_read`,"
                "sum(`performance_schema`.`file_summary_by_instance`.`COUNT_WRITE`) AS `count_write`,"
                "sum(`performance_schema`.`file_summary_by_instance`.`SUM_NUMBER_OF_BYTES_WRITE`) AS `sum_number_of_bytes_write`,"
                "sum(`performance_schema`.`file_summary_by_instance`.`SUM_TIMER_WRITE`) AS `sum_timer_write`,"
                "sum(`performance_schema`.`file_summary_by_instance`.`COUNT_MISC`) AS `count_misc`,"
                "sum(`performance_schema`.`file_summary_by_instance`.`SUM_TIMER_MISC`) AS `sum_timer_misc` "
                "from `performance_schema`.`file_summary_by_instance` group by `table_schema`,`table_name`;\n"
        )
        print("query: ", ret)
        compare_lower(ret, "CREATE ALGORITHM=TEMPTABLE DEFINER=`mysql.sys`@`localhost` SQL SECURITY INVOKER "
                           "VIEW `x$ps_schema_table_statistics_io` AS "
                           "SELECT 1  AS `table_schema`,"
                           "1  AS `table_name`,"
                           "1  AS `count_read`,"
                           "1  AS `sum_number_of_bytes_read`,"
                           "1  AS `sum_timer_read`,"
                           "1  AS `count_write`,"
                           "1  AS `sum_number_of_bytes_write`,"
                           "1  AS `sum_timer_write`,"
                           "1  AS `count_misc`;\n")


if __name__ == '__main__':
    unittest.main()
