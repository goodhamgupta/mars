REGISTRY_CREATE = """
  CREATE TABLE {registry_table} (
    Id INT NOT NULL AUTO_INCREMENT,
    current_run_at TIMESTAMP NOT NULL,
    last_run_at TIMESTAMP NOT NULL,
    succeeded TINYINT DEFAULT NULL,
    PRIMARY KEY (id)
  );
"""

REGISTRY_EXIST = """
  SHOW TABLES LIKE '{registry_table}';
"""

REGISTRY_INSERT = """
  INSERT INTO
    {registry_table} (last_run_at, current_run_at, succeeded)
  VALUES('{snapshot_start}', '{snapshot_end}', 0)
"""

REGISTRY_SUCCEEDED = """
  UPDATE {registry_table}
  SET succeeded = 1
  WHERE
    succeeded IS NULL
    OR succeeded = 0
    AND current_run_at = '{snapshot_start}'
    AND last_run_at = '{snapshot_end}'
"""

REGISTRY_FAILURE = """
  UPDATE {registry_table}
  SET succeeded = 0
  WHERE succeeded IS NULL
"""

REGISTRY_ALTER = """
  ALTER TABLE {registry_table}
  ADD COLUMN succeeded TINYINT DEFAULT NULL;
"""

REGISTRY_LATEST_RUN = """
  SELECT
    last_run_at
  FROM {registry_table}
  ORDER BY id DESC
  LIMIT 1;
"""

REGISTRY_PENDINGS = """
  SELECT
  MIN(last_run_at)
    FROM {registry_table}
    WHERE
  succeeded=0;
"""

REGISTRY_SELECT_MAX = """
  SELECT
    current_run_at
  FROM {registry_table}
  ORDER BY id DESC
  LIMIT 1;
"""

CREATE_TMP = """
  -- Dummy CREATE_TMP statement
"""

CREATE = """
  -- Dummy CREATE statement
"""

REPLICATE = """
  -- Dummy REPLICATE statement
"""

INSERT = """
  -- Dummy INSERT statement
"""

SET = """
  -- Dummy SET statement
"""

SET = """
  -- Dummy SET statement
"""

DELETE_TMP = """
  -- Dummy DELETE_TMP statement
"""

PURGE = """
  -- Dummy PURGE statement
"""

REMOVE_TMP = """
  -- Dummy REMOVE_TMP statement
"""

COUNT = """
  -- Dummy COUNT statement
"""
