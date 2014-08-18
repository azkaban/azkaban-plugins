SELECT 'Upgrading MetaStore schema from 0.11.0 to 0.1-c5b11.0-c5b1' AS Status from dual;
UPDATE VERSION SET SCHEMA_VERSION='0.12.0', VERSION_COMMENT='Hive release version 0.12.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 0.11.0-c5b1 to 0.12.0' AS Status from dual;

