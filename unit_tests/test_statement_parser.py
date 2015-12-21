import unittest

from mysql.utilities.command.dbexport import create_preliminary_view


def compare_lower(a, b):
    assert a.lower() == b.lower()


class ViewParserTest(unittest.TestCase):
    """
    Test generation of preparatory views.
    """
    def test_preliminary_view_basic(self):
        ret = create_preliminary_view(
                "CREATE VIEW myview AS select `t`.`a` AS `a`, `t`.`b` AS `b` FROM t;\n")
        compare_lower(ret, "CREATE VIEW myview AS SELECT 1  AS `a`,1  AS `b`;\n")

    def test_preliminary_view_union(self):
        # SELECT UNION uses only column names from the first view.
        ret = create_preliminary_view(
                "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` SQL SECURITY DEFINER "
                "VIEW `logins` AS"
                " select `db_one`.`table_b`.`col_a` AS `col_a`,"
                "`db_one`.`table_b`.`col_be` AS `col_be`,"
                "'STATIC_VALUE' AS `label_c` "
                "from (`db_one`.`table_b` JOIN `db_two`.`table_three` on"
                "((`db_one`.`table_b`.`col_z` = `db_two`.`table_three`.`col_z`))) "
                "union select `db_one`.`table_f`.`col_a` AS `col_a`,"
                "`db_one`.`table_f`.`col_be` AS `col_be`,"
                "'ANOTHER_STATIC' AS `label_c_bis` "
                "from `db_one`.`table_f`;\n ")
        compare_lower(ret, "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` SQL SECURITY DEFINER "
                           "VIEW `logins` AS "
                           "SELECT 1  AS `col_a`,"
                           "1  AS `col_be`,"
                           "1  AS `label_c`"
                           ";\n")

    def test_preliminary_view_sys(self):
        ret = create_preliminary_view(
                "CREATE ALGORITHM=TEMPTABLE DEFINER=`mysql.sys`@`localhost` SQL SECURITY INVOKER "
                "VIEW `x$ps_schema_table_statistics_io` AS"
                " select `extract_schema_from_file_name`(`performance_schema`.`file_summary_by_instance`.`FILE_NAME`) AS `table_schema`,"
                "`extract_table_from_file_name`(`performance_schema`.`file_summary_by_instance`.`FILE_NAME`) AS `table_name`,"
                "sum(`performance_schema`.`file_summary_by_instance`.`COUNT_READ`) AS `count_read`,"
                "sum(`performance_schema`.`file_summary_by_instance`.`SUM_NUMBER_OF_BYTES_READ`) AS `sum_number_of_bytes_read`,"
                "from `performance_schema`.`file_summary_by_instance` group by `table_schema`,`table_name`;\n"
        )
        compare_lower(ret, "CREATE ALGORITHM=TEMPTABLE DEFINER=`mysql.sys`@`localhost` SQL SECURITY INVOKER "
                           "VIEW `x$ps_schema_table_statistics_io` AS "
                           "SELECT 1  AS `table_schema`,"
                           "1  AS `table_name`,"
                           "1  AS `count_read`,"
                           "1  AS `sum_number_of_bytes_read`"
                           ";\n")


if __name__ == '__main__':
    unittest.main()
