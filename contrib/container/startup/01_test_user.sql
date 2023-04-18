SET SERVEROUTPUT ON SIZE 1000000 
SET TIMI ON ECHO ON

WHENEVER SQLERROR CONTINUE

ALTER SYSTEM SET db_create_file_dest = '/opt/oracle/oradata/FREE';
CREATE PLUGGABLE DATABASE testpdb
  ADMIN USER test_admin IDENTIFIED BY r97oUPimsmTOIcBaeeDF;

ALTER PLUGGABLE DATABASE testpdb OPEN;

ALTER SESSION SET CONTAINER = testpdb;

CREATE TABLESPACE data 
  DATAFILE '/opt/oracle/oradata/FREE/testpdb-data01.dbf' 
  SIZE 32M
  AUTOEXTEND ON NEXT 1M;


DROP USER test CASCADE;

WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK

CREATE USER test IDENTIFIED BY r97oUPimsmTOIcBaeeDF;
GRANT create session, create table, create type, create sequence, create synonym, create procedure, change notification TO test;
GRANT EXECUTE ON SYS.DBMS_AQ TO test;
GRANT EXECUTE ON SYS.DBMS_AQADM TO test;

GRANT create user, drop user, alter user TO test;
GRANT connect TO test WITH admin option;
GRANT create session TO test WITH admin option;

WHENEVER SQLERROR CONTINUE

ALTER USER test QUOTA 100m ON system;
ALTER USER test QUOTA 100m ON data;

EXIT
