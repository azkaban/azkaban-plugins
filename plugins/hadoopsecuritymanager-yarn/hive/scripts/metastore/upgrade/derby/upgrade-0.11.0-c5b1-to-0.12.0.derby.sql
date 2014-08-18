-- Upgrade MetaStore schema from 0.11.0-c5b1 to 0.12.0
UPDATE "APP".VERSION SET SCHEMA_VERSION='0.12.0', VERSION_COMMENT='Hive release version 0.12.0' where VER_ID=1;
