import unittest

from mysql.utilities.command.dbexport import create_preliminary_view


def assert_equal_ci(a, b):
    assert a.lower().strip("; \n") == b.lower().strip("; \n"), "%r vs %r" % (a, b)


class ViewParserTest(unittest.TestCase):
    """
    Test generation of preliminary views.
    """

    def test_preliminary_view_basic_without_from(self):
        ret = create_preliminary_view(
                "CREATE VIEW myview AS select `t`.`a` AS `a`, `t`.`b` AS `b`")
        assert_equal_ci(ret, "CREATE VIEW myview AS SELECT 1  AS `a`,1  AS `b`")

    def test_preliminary_view_basic_with_from(self):
        ret = create_preliminary_view(
                "CREATE VIEW myview AS select `t`.`a` AS `a`, `t`.`b` AS `b` FROM t")
        assert_equal_ci(ret, "CREATE VIEW myview AS SELECT 1  AS `a`,1  AS `b`")

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
                "from `db_one`.`table_f`")
        assert_equal_ci(ret, "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` SQL SECURITY DEFINER "
                           "VIEW `logins` AS "
                           "SELECT 1  AS `col_a`,"
                           "1  AS `col_be`,"
                           "1  AS `label_c`"
                           "")

    def test_preliminary_view_sys(self):
        # Test with a complex sys view.
        ret = create_preliminary_view(
                "CREATE ALGORITHM=TEMPTABLE DEFINER=`mysql.sys`@`localhost` SQL SECURITY INVOKER "
                "VIEW `x$ps_schema_table_statistics_io` AS"
                " select `extract_schema_from_file_name`(`performance_schema`.`file_summary_by_instance`.`FILE_NAME`) AS `table_schema`,"
                "`extract_table_from_file_name`(`performance_schema`.`file_summary_by_instance`.`FILE_NAME`) AS `table_name`,"
                "sum(`performance_schema`.`file_summary_by_instance`.`COUNT_READ`) AS `count_read`,"
                "sum(`performance_schema`.`file_summary_by_instance`.`SUM_NUMBER_OF_BYTES_READ`) AS `sum_number_of_bytes_read`"
                " from `performance_schema`.`file_summary_by_instance` group by `table_schema`,`table_name`"
        )
        assert_equal_ci(ret, "CREATE ALGORITHM=TEMPTABLE DEFINER=`mysql.sys`@`localhost` SQL SECURITY INVOKER "
                           "VIEW `x$ps_schema_table_statistics_io` AS "
                           "SELECT 1  AS `table_schema`,"
                           "1  AS `table_name`,"
                           "1  AS `count_read`,"
                           "1  AS `sum_number_of_bytes_read`"
                           "")

    def test_preliminary_view_subquery(self):
        # Test with a large complex sys view.
        ret = create_preliminary_view("""CREATE algorithm=temptable definer=`mysql.sys`@`localhost` SQL security invoker view `metrics`
AS
  (
         SELECT lower(`performance_schema`.`global_status`.`variable_name`) AS `variable_name`,
                `performance_schema`.`global_status`.`variable_value`       AS `variable_value`,
                'Global Status'                                             AS `type`,
                'YES'                                                       AS `enabled`
         FROM   `performance_schema`.`global_status`)
  UNION ALL
            (
                   SELECT `information_schema`.`innodb_metrics`.`name`                                         AS `variable_name`,
                          `information_schema`.`innodb_metrics`.`count`                                        AS `variable_value`,
                                 concat('InnoDB Metrics - ',`information_schema`.`innodb_metrics`.`subsystem`) AS `type`,
                          IF((`information_schema`.`innodb_metrics`.`status` = 'enabled'),'YES','NO')          AS `enabled`
                   FROM   `information_schema`.`innodb_metrics`
                   WHERE  (
                                 `information_schema`.`innodb_metrics`.`name` NOT IN ('lock_row_lock_time',
                                                                                      'lock_row_lock_time_avg',
                                                                                      'lock_row_lock_time_max',
                                                                                      'lock_row_lock_waits',
                                                                                      'buffer_pool_reads',
                                                                                      'buffer_pool_read_requests',
                                                                                      'buffer_pool_write_requests',
                                                                                      'buffer_pool_wait_free',
                                                                                      'buffer_pool_read_ahead',
                                                                                      'buffer_pool_read_ahead_evicted',
                                                                                      'buffer_pool_pages_total',
                                                                                      'buffer_pool_pages_misc',
                                                                                      'buffer_pool_pages_data',
                                                                                      'buffer_pool_bytes_data',
                                                                                      'buffer_pool_pages_dirty',
                                                                                      'buffer_pool_bytes_dirty',
                                                                                      'buffer_pool_pages_free',
                                                                                      'buffer_pages_created',
                                                                                      'buffer_pages_written',
                                                                                      'buffer_pages_read',
                                                                                      'buffer_data_reads',
                                                                                      'buffer_data_written',
                                                                                      'file_num_open_files',
                                                                                      'os_log_bytes_written',
                                                                                      'os_log_fsyncs',
                                                                                      'os_log_pending_fsyncs',
                                                                                      'os_log_pending_writes',
                                                                                      'log_waits',
                                                                                      'log_write_requests',
                                                                                      'log_writes',
                                                                                      'innodb_dblwr_writes',
                                                                                      'innodb_dblwr_pages_written',
                                                                                      'innodb_page_size')))
     UNION ALL
               (
                      SELECT 'memory_current_allocated'                                                                     AS `variable_name`,
                             sum(`performance_schema`.`memory_summary_global_by_event_name`.`current_number_of_bytes_used`) AS `variable_value`,
                             'Performance Schema'                                                                           AS `type`,
                             IF((
                                  (
                                  SELECT count(0)
                                  FROM   `performance_schema`.`setup_instruments`
                                  WHERE  ((
                                                       `performance_schema`.`setup_instruments`.`name` LIKE 'memory/%')
                                         AND    (
                                                       `performance_schema`.`setup_instruments`.`enabled` = 'YES'))) = 0),'NO',IF((
                                                                                                                                    (
                                                                                                                                    SELECT count(0)
                                                                                                                                    FROM   `performance_schema`.`setup_instruments`
                                                                                                                                    WHERE  ((
                                                                                                                                                         `performance_schema`.`setup_instruments`.`name` LIKE 'memory/%')
                                                                                                                                           AND    (
                                                                                                                                                         `performance_schema`.`setup_instruments`.`enabled` = 'YES'))) =
                                                                                                                                   (
                                                                                                                                          SELECT count(0)
                                                                                                                                          FROM   `performance_schema`.`setup_instruments`
                                                                                                                                          WHERE  (
                                                                                                                                                        `performance_schema`.`setup_instruments`.`name` LIKE 'memory/%'))),'YES','PARTIAL')) AS `enabled`
                      FROM   `performance_schema`.`memory_summary_global_by_event_name`)
        UNION ALL
                  (
                         SELECT 'memory_total_allocated'                                                                    AS `variable_name`,
                                sum(`performance_schema`.`memory_summary_global_by_event_name`.`sum_number_of_bytes_alloc`) AS `variable_value`,
                                'Performance Schema'                                                                        AS `type`,
                                IF((
                                     (
                                     SELECT count(0)
                                     FROM   `performance_schema`.`setup_instruments`
                                     WHERE  ((
                                                          `performance_schema`.`setup_instruments`.`name` LIKE 'memory/%')
                                            AND    (
                                                          `performance_schema`.`setup_instruments`.`enabled` = 'YES'))) = 0),'NO',IF((
                                                                                                                                       (
                                                                                                                                       SELECT count(0)
                                                                                                                                       FROM   `performance_schema`.`setup_instruments`
                                                                                                                                       WHERE  ((
                                                                                                                                                            `performance_schema`.`setup_instruments`.`name` LIKE 'memory/%')
                                                                                                                                              AND    (
                                                                                                                                                            `performance_schema`.`setup_instruments`.`enabled` = 'YES'))) =
                                                                                                                                      (
                                                                                                                                             SELECT count(0)
                                                                                                                                             FROM   `performance_schema`.`setup_instruments`
                                                                                                                                             WHERE  (
                                                                                                                                                           `performance_schema`.`setup_instruments`.`name` LIKE 'memory/%'))),'YES','PARTIAL')) AS `enabled`
                         FROM   `performance_schema`.`memory_summary_global_by_event_name`)
           UNION ALL
                     (
                            SELECT 'NOW()'       AS `variable_name`,
                                   now(3)        AS `variable_value`,
                                   'System Time' AS `type`,
                                   'YES'         AS `enabled`)
              UNION ALL
                        (
                               SELECT 'UNIX_TIMESTAMP()'              AS `variable_name`,
                                      round(unix_timestamp(now(3)),3) AS `variable_value`,
                                      'System Time'                   AS `type`,
                                      'YES'                           AS `enabled`)
              ORDER BY  `type`,
                        `variable_name`""")
        assert_equal_ci(ret,
                      "CREATE algorithm=temptable definer=`mysql.sys`@`localhost` SQL security invoker"
                      " VIEW `metrics` AS SELECT 1  AS `variable_name`,"
                      "1  AS `variable_value`,"
                      "1  AS `type`,"
                      "1  AS `enabled`")


if __name__ == '__main__':
    unittest.main()
