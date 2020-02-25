from hiveql.common import *

CREATE_TMP = """
    DROP TABLE IF EXISTS {table};
    CREATE TABLE {table} (
        derived_tstamp timestamp,
        derived_tstamp_month int,
        derived_tstamp_year int,
        collector_tstamp timestamp,
        etl_tstamp timestamp,
        event_id string,
        domain_userid string,
        user_id string,
        domain_sessionid string,
        domain_sessionidx integer,
        app_id string,
        platform string,
        se_category string,
        se_action string,
        se_label string,
        se_property string,
        se_value float,
        run string
    )
"""

CREATE = """
    CREATE EXTERNAL TABLE IF NOT EXISTS {external_table} (
        derived_tstamp timestamp,
        collector_tstamp timestamp,
        etl_tstamp timestamp,
        event_id string,
        domain_userid string,
        user_id string,
        domain_sessionid string,
        domain_sessionidx integer,
        app_id string,
        platform string,
        se_category string,
        se_action string,
        se_label string,
        se_property string,
        se_value float,
        run string
    )
    PARTITIONED BY (derived_tstamp_month int, derived_tstamp_year int)
    STORED AS ORC
    LOCATION "{external_table_location}"
"""

REPLICATE = """
    INSERT INTO {table} (
        SELECT derived_tstamp,
               month(derived_tstamp) as derived_tstamp_month,
               year(derived_tstamp) as derived_tstamp_year,
               collector_tstamp,
               etl_tstamp,
               event_id,
               domain_userid,
               user_id,
               domain_sessionid,
               domain_sessionidx,
               app_id,
               platform,
               se_category,
               se_action,
               se_label,
               se_property,
               se_value,
               run
        FROM {source_table}
        WHERE se_category LIKE 'offering%'
        AND run >= '{snapshot_start}' AND run < '{snapshot_end}'
    )
"""

INSERT = """
    SET hive.exec.dynamic.partition.mode=nonstrict;
    SET hive.exec.dynamic.partition=true;
    INSERT INTO {external_table} partition(derived_tstamp_month, derived_tstamp_year) (
        SELECT derived_tstamp,
               collector_tstamp,
               etl_tstamp,
               event_id,
               domain_userid,
               user_id,
               domain_sessionid,
               domain_sessionidx,
               app_id,
               platform,
               se_category,
               se_action,
               se_label,
               se_property,
               se_value,
               run,
               derived_tstamp_month,
               derived_tstamp_year
        FROM {table}
        WHERE run >= '{snapshot_start}' AND run < '{snapshot_end}'
    )
"""

DELETE_TMP = """
    -- Dummy {source_table}
    DELETE FROM {table}
    WHERE
      run >= '{snapshot_start}' AND run < '{snapshot_end}'
"""
PURGE = """
    DELETE FROM {table}
"""

REMOVE_TMP = """
    DROP TABLE {table}
"""

COUNT = """
    SELECT COUNT(1) from {table}
"""
