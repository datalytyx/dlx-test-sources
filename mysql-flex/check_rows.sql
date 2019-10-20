SET @table_schema = DATABASE();
-- or SET @table_schema = 'my_db_name';

SET GROUP_CONCAT_MAX_LEN=131072;
SET @selects = NULL;

SELECT GROUP_CONCAT(
        'SELECT "', table_name,'" as TABLE_NAME, COUNT(*) as TABLE_ROWS FROM `', table_name, '`'
        SEPARATOR '\nUNION\n') INTO @selects
  FROM information_schema.TABLES
  WHERE TABLE_SCHEMA = @table_schema
        AND ENGINE = 'InnoDB'
        AND TABLE_TYPE = "BASE TABLE";

SELECT CONCAT_WS('\nUNION\n',
  CONCAT('SELECT TABLE_NAME, TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND ENGINE <> "InnoDB" AND TABLE_TYPE = "BASE TABLE"'),
  @selects) INTO @selects;

PREPARE stmt FROM @selects;
EXECUTE stmt USING @table_schema;
DEALLOCATE PREPARE stmt;
