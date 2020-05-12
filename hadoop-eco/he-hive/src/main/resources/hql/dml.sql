LOAD DATA   -- 加载数据
[LOCAL] -- 从本地加载数据到hive表，否则从 hdfs加载数据到hive表
    INPATH  -- 加载数据的路径
    hdfs_path
    [OVERWRITE] -- 覆盖表中已有的数据，否则表示追加
    INTO TABLE  -- 加载到哪张表
    table_name  -- 具体表名
    [PARTITION(p_caol1 = val1,...)];    -- 上传到指定分区

SHOW DATABASES ;
SHOW TABLES ;

CREATE DATABASE IF NOT EXISTS dml_db_hive
    COMMENT 'HIVE DML DATABASE'
    LOCATION '/db_hive/dml_db_hive' ;

USE dml_db_hive ;

CREATE TABLE IF NOT EXISTS dml_student_i(
                                            id string,
                                            name string
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE

;

ALTER TABLE dml_student SET TBLPROPERTIES ('EXTERNAL' ='TRUE');

dfs -put /export/testData/student.txt /user/yi/hive;
dfs -rm -r /db_hive/dml_db_hive/dml_student_i ;
DESC FORMATTED dml_student_e;
DROP TABLE dml_student_i;
SELECT * FROM dml_student_e;

-- COMMENT '数据导入
-- COMMENT '向表中装载数据'
-- 从本地文件到dml_student表
LOAD DATA LOCAL INPATH
    '/export/testData/student.txt'
    INTO TABLE
        dml_student;

-- 从hdfs文件到dml_student表
LOAD DATA INPATH
    '/user/yi/hive/student.txt'
    INTO TABLE
        dml_student_e ;

-- 加载数据覆盖dml_student表中数据
LOAD DATA INPATH
    '/user/yi/hive/student.txt'
    OVERWRITE INTO TABLE
    dml_student ;

-- COMMENT 'INSERT 通过查询语句创建表并加载数据'
-- INSERT INTO(追加的效果)
-- INSERT OVERWRITE(覆盖表或者分区中已经存在的数据)
-- INSERT 不支持插入部分字段
CREATE TABLE IF NOT EXISTS dml_student_partition_rn(
                                                       id int,
                                                       name string
)
    PARTITIONED BY (month string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE ;
-- 基本插入数据
INSERT INTO TABLE dml_student_partition
    PARTITION
(month='201812')
VALUES
(1,"WW"),
(2,'ZL');
SELECT * FROM dml_student_partition WHERE month = '201812';
ALTER TABLE dml_student_partition DROP PARTITION (month='');
SHOW PARTITIONS dml_student_partition;
DESC FORMATTED dml_student_partition;


-- 根据单张表查询结果
INSERT OVERWRITE TABLE dml_student_partition
    PARTITION
(month = '201811')
VALUES
(3,"XX"),
(4,"DD");
SELECT * FROM dml_student_partition WHERE month = '201811';

-- 多表（多分区）插入，根据多张表的查询结果
FROM dml_student_partition
INSERT OVERWRITE TABLE dml_student_partition PARTITION (month = '201810')
SELECT id,name WHERE month = '201812'
INSERT OVERWRITE TABLE dml_student_partition PARTITION (month = '201809')
SELECT id,name WHERE month = '201811';
SELECT * FROM dml_student_partition;

-- COMMENT 'AS SELECT 通过查询语句创建表并加载数据'
CREATE TABLE IF NOT EXISTS
    dml_student_as
AS
SELECT
    id,name
FROM
    dml_student_partition
WHERE
        month = '201809' ;
SELECT * FROM dml_student_as;

-- COMMENT '创建表时通过 LOCATION 指定加载数据路径'
dfs -rm -r /db_hive/dml_db_hive/dml_student_location;
dfs -mkdir /db_hive/dml_db_hive/dml_student_location;
dfs -put /export/testData/student.txt /db_hive/dml_db_hive/dml_student_location;
CREATE TABLE IF NOT EXISTS dml_student_location(
                                                   id int,
                                                   name string
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/db_hive/dml_db_hive/dml_student_location';
ALTER TABLE dml_student_location SET TBLPROPERTIES ('EXTERNAL' = 'TRUE');
SELECT * FROM dml_student_location ;

-- COMMENT 'IMPORT 数据到指定Hive表中'
dfs -rm -r /user/yi/hive/export/student;
dfs -mkdir /user/yi/hive/export;
dfs -mkdir /user/yi/hive/export/student_partition;
EXPORT TABLE dml_student_partition  to '/user/yi/hive/export/student_partition';
IMPORT TABLE dml_student_partition_rn PARTITION (month='201810') FROM '/user/yi/hive/export/student_partition'
;

-- COMMENT '数据导出'
-- 将查询结果导出到本地
INSERT OVERWRITE
    LOCAL DIRECTORY '/export/testData/export/student'
SELECT
    *
FROM
    default.student;
-- 将查询结果格式化导出到本地
INSERT OVERWRITE LOCAL DIRECTORY '/export/testData/export/dml_student'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * FROM dml_student;

-- 将查询结果导出到hdfs
INSERT OVERWRITE DIRECTORY '/user/yi/hive/export/student'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * FROM default.student;

-- hadoop 命令导出到本地
dfs -get
    /user/yi/hive/export/student_partition/month=201809/000000_0
    /export/testData/export/dml_student_partition_student_201809.txt;
-- hive shell命令导出
-- bin/hive -f/-e 执行语句或者脚本 > file
-- bin/hive -e 'select * from default.student;' > '/export/testData/export/default_student.txt';

-- EXPORT导出到hdfs上
EXPORT TABLE dml_student to '/user/yi/hive/export/dml_student';

-- sqoop 导出


-- COMMENT '数据清除' 只能清除管理表的数据，不能清除外部表的数据
create external table if not exists ddl_db_test.truncate_test(i int);
create  table if not exists ddl_db_test.truncate_test0(i int);
INSERT INTO TABLE ddl_db_test.truncate_test VALUES (1);
INSERT INTO TABLE ddl_db_test.truncate_test0 VALUES (0);
SELECT * FROM ddl_db_test.truncate_test;
SELECT * FROM ddl_db_test.truncate_test0;
USE ddl_db_test;
show tables;
truncate table ddl_db_test.truncate_test0;