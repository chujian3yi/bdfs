---------------------------------------------------------
---------------------------------------------------------
--COMMENT 创建数据库语法
CREATE DATABASE [IF NOT EXISTS] database_name
[COMMENT database_comment]
[LOCATION hdfs_path]
[WITH DBPROPERTIES(p_name=p_value,...)] ;

---------------------------------------------------------------------
--COMMENT #1 创建数据库
CREATE DATABASE IF NOT EXISTS database_name
COMMENT
    'HIVE 创建数据库'
LOCATION
    '/db_hive/database_name' ;

---------------------------------------------------------------------
--COMMENT #2 查询数据库
SHOW DATABASE ;
SHOW DATABASE like 'db*' ;

DESC DATABASE database_name ;
DESC DATABASE EXTENDED database_name ;

---------------------------------------------------------------------
--COMMENT #3 修改数据库
ALTER DATABASE database_name
SET
    dbproperties('create_time'='20200101') ;

---------------------------------------------------------------------
--COMMENT #4 删除数据库
DROP DATABASE database_name ;
DROP DATABASE IF EXISTS database_name ;
DROP DATABASE IF EXISTS database_name cascade ;

---------------------------------------------------------
---------------------------------------------------------
-- COMMENT '创建数据库'
CREATE DATABASE IF NOT EXISTS ddl_db_test
COMMENT 'HIVE 创建数据库'
LOCATION '/db_hive/ddl_db_test' ;
-- COMMENT '显示数据库'
SHOW DATABASES ;
-- COMMENT '过滤显示查询数据库'
SHOW DATABASES LIKE 'ddl_db*' ;
-- COMMENT '显示数据库信息'
DESC DATABASE ddl_db_test ;
-- COMMENT '显示数据库详细信息'
DESC DATABASE EXTENDED  ddl_db_test ;
-- COMMENT '修改数据库'
ALTER DATABASE ddl_db_test
    SET
        dbproperties('create_time'='20200101') ;
-- COMMENT '删除数据库'
DROP DATABASE IF EXISTS ddl_db_test ;
---------------------------------------------------------
---------------------------------------------------------


---------------------------------------------------------
---------------------------------------------------------
--COMMENT 创建表
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] table_name
[(col_name data_type [COMMENT col_comment],...)]
[COMMENT table_comment]
[PARTITIONED BY (col_name,col_name,...)]
[CLUSTERED BY (col_name,col_namee,...)
    [SORTED BY (col_name[ASC|DESC]),...] INTO num_buckets BUCKETS]
[ROW FORMATE row_format]
    row_format:
    {
        -- 行格式
        DELIMITED [FIELDS TERMINATED BY char]
                  [COLLECTION ITEMS TERMINATED BY char ]
                  [MAP KEYS TERMINATED BY char]
                  [LINES TERMINATED BY char]
        -- 对象的序列化和反序列化
        SERDE serde_name[WITH SERDEPROPERTIES(p_name=p_value,...)]
    }
[STORED AS file_format]
    file_format:
    {
        [SEQUENCEFILE]      -- 二进制序列文件
        [TEXITFILE]         -- 文本
        [RCFILE]            -- 列式存储格式文件
    }


---------------------------------------------------------------------
-- COMMENT '指定表在 HDFS 上的存储位置'
[LOCATION hdfs_path]
[TBLPROPERTIES (p_name = p_value,...)]
[As select_statement] ;

---------------------------------------------------------------------
-- COMMENT '创建普通表'
CREATE TABLE IF NOT EXISTS table_name (
    id int,
    name string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/db_hive/database_name/table_name' ;

---------------------------------------------------------------------
-- COMMENT '根据查询结果创建表（查询结果添加到新创建的表中）'
CREATE TABLE IF NOT EXISTS table_name AS select_statement;

---------------------------------------------------------------------
-- COMMENT '根据已经存在的表结构创建新表'
CREATE TABLE IF NOT EXISTS new_table_name LIKE old_table_name;

---------------------------------------------------------------------
-- COMMENT '查询表的类型'
DESC FORMATTED table_name ;

---------------------------------------------------------------------
-- COMMENT '外部表 external'
CREATE TABLE IF NOT EXISTS EXTERNAL table_name(
    col_name col_type,
    ...
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED BY  SEQUENCEFILE
LOCATION '/db_hive/database_name/table_name' ;

---------------------------------------------------------------------
-- COMMENT '管理表和外部表转换'
DESC FORMATTED table_name ;
    -- 修改内部表为外部表
ALTER TABLE
    table_name
SET TBPROPERTIES('EXTERNAL' = 'TRUE') ;
    -- 修改外部表为内部表
ALTER TABLE
    table_name
SET TBPROPERTIES('EXTERNAL' = 'FALSE') ;

---------------------------------------------------------------------
-- COMMENT '分区表'
USE ddl_db_hive ;
CREATE TABLE IF NOT EXISTS ddl_dept_partition(
    dep_tno int,
    dep_name string,
    dep_loc string
)
PARTITIONED BY (month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;

---------------------------------------------------------------------
-- COMMENT '加载数据'
load data local inpath '/export/testData/dept.txt' into table ddl_db_hive.ddl_dept_partition partition(month='201909');
load data local inpath '/export/testData/dept.txt' into table ddl_db_hive.ddl_dept_partition partition(month='201908');
load data local inpath '/export/testData/dept.txt' into table ddl_db_hive.ddl_dept_partition partition(month='201907');

---------------------------------------------------------------------
-- COMMENT '查询分区表数据'
SELECT * FROM ddl_dept_partition where month  = '201909';

---------------------------------------------------------------------
-- COMMENT '多分区联合查询'
SELECT * FROM ddl_dept_partition where month  = '201909'
UNION ALL
SELECT * FROM ddl_dept_partition where month  = '201908'
UNION ALL
SELECT * FROM ddl_dept_partition where month  = '201907';

---------------------------------------------------------------------
-- COMMENT '增加分区'
ALTER TABLE ddl_dept_partition ADD partition(month = '201906');
ALTER TABLE ddl_dept_partition ADD partition(month = '201905')partition(month = '201904')

---------------------------------------------------------------------
-- COMMENT '删除分区'
ALTER TABLE ddl_dept_partition DROP partition(month = '201906');
ALTER TABLE ddl_dept_partition DROP partition(month = '201905'),partition(month = '201904');

---------------------------------------------------------------------
-- COMMENT '查看分区表分区数'
SHOW partitions ddl_dept_partition;

---------------------------------------------------------------------
-- COMMENT '查看分区表结构'
DESC FORMATTED ddl_dept_partition;

---------------------------------------------------------------------
-- COMMENT '创建二级分区表'
USE ddl_db_hive;
CREATE TABLE IF NOT EXISTS ddl_dept_partition_2(
    dept_no int,
    dept_name string,
    dept_loc string
)
PARTITIONED BY(month string,day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

---------------------------------------------------------------------
-- COMMENT '加载数据到分区表的方式一'
LOAD DATA LOCAL
    INPATH '/export/testData/dept.txt'
INTO TABLE
    ddl_db_hive.ddl_dept_partition_2
PARTITION(month = '201903',day = '01');

---------------------------------------------------------------------
-- COMMENT '查询分区数据'
SELECT
    *
FROM
    ddl_dept_partition_2
WHERE
    month = '201903' AND day = '01' ;
    desc formatted ddl_dept_partition_2;

---------------------------------------------------------------------
-- COMMENT '把数据直接上传到分区目录上，让分区表和数据产生关联的三种方式'
-- 方式一，上传数据后修复
dfs -mkdir -p /db_hive/ddl_db_hive/ddl_dept_partition_2/month=201903/day=02;
dfs -put /export/testData/dept.txt /db_hive/ddl_db_hive/ddl_dept_partition_2/month=201903/day=02;
-- 执行修复命令
MSCK REPAIR TABLE ddl_dept_partition_2;
-- 查询数据
SELECT
       *
FROM
     ddl_dept_partition_2
WHERE
      month = '201903'
AND
      day = '02';

-- 方式二，上传数据后添加分区
dfs -mkdir -p /db_hive/ddl_db_hive/ddl_dept_partition_2/month=201903/day=03;
dfs -put /export/testData/dept.txt /db_hive/ddl_db_hive/ddl_dept_partition_2/month=201903/day=03;
-- 执行添加分区
ALTER TABLE
    ddl_dept_partition_2
    ADD PARTITION (month = '201903',day = '03');
-- 查询数据
SELECT
    *
FROM
    ddl_dept_partition_2
WHERE
        month = '201903'
  AND
        day = '03';

-- 方式三，创建文件夹后 load 数据到分区
-- 创建文件夹
dfs -mkdir -p /db_hive/ddl_db_hive/ddl_dept_partition_2/month=201903/day=04;
-- 上传数据 load
LOAD DATA LOCAL INPATH
    '/export/testData/dept.txt'
INTO TABLE
    ddl_dept_partition_2
    PARTITION (month = '201903',day = '04');
-- 查询数据
SELECT
    *
FROM
    ddl_dept_partition_2
WHERE
        month = '201903'
  AND
        day = '04';


show tables;

---------------------------------------------------------------------
-- COMMENT '修改表'
-- 重命名表
ALTER TABLE table_name
    RENAME TO new_table_name ;
-- 更新列
ALTER TABLE table_name
    CHANGE[column] col_old_name col_new_name column_type
    [COMMENT col_comment]
    [FIRST|AFTER column_name];
-- 增加或替换列
ALTER TABLE table_name ADD | REPLACE COLUMNS(col_name column_type
    [COMMENT col_comment],...)

show tables;
desc ddl_db_hive.ddl_dept_partition_2;

---------------------------------------------------------------------
-- COMMENT '修改表'
-- 重命名表
ALTER TABLE ddl_db_hive.ddl_dept_partition
    RENAME TO ddl_dept_partition_1 ;
ALTER TABLE ddl_db_hive.ddl_dept_partition_1
    RENAME TO ddl_dept_partition ;
-- 增加或替换列
ALTER TABLE ddl_db_hive.ddl_dept_partition ADD COLUMNS (deptdesc int);
ALTER TABLE ddl_db_hive.ddl_dept_partition REPLACE COLUMNS (dept_no int,dept_name string,dept_loc string,dept_desc string);
-- 更新列
ALTER TABLE ddl_db_hive.ddl_dept_partition CHANGE COLUMN deptdesc dept_desc int;

---------------------------------------------------------------------
-- COMMENT '删除表'
DROP TABLE table_name;


---------------------------------------------------------
---------------------------------------------------------

-- COMMENT '拉链表'

-- COMMENT '增量表'

-- COMMENT '全量表'

-- COMMENT '动态分区表'


---------------------------------------------------------
---------------------------------------------------------
---------------------------------------------------------
---------------------------------------------------------
-- COMMENT '创建数据库'
CREATE DATABASE IF NOT EXISTS ddl_db_test
COMMENT 'HIVE 创建数据库'
LOCATION '/db_hive/ddl_db_test';

-- COMMENT '显示数据库'
SHOW DATABASES ;

-- COMMENT '过滤显示查询数据库'
SHOW DATABASES LIKE 'ddl_db*' ;

-- COMMENT '显示数据库信息'
DESC DATABASE ddl_db_hive ;

-- COMMENT '显示数据库详细信息'
DESC DATABASE EXTENDED  ddl_db_test ;

-- COMMENT '修改数据库'
ALTER DATABASE ddl_db_test
    SET
        dbproperties('create_time'='20200101') ;

-- COMMENT '删除数据库'
DROP DATABASE IF EXISTS ddl_db_hive ;





-- COMMENT '在ddl_db_hive数据库创建普通表'
USE ddl_db_hive ;
CREATE TABLE IF NOT EXISTS ddl_db_hive.ddl_student0(
                                          id int,
                                          name string
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/db_hive/ddl_db_hive/ddl_student0' ;

-- COMMENT '根据查询结果创建表（查询结果会添加到新创建的表中）'
USE ddl_db_hive;
CREATE TABLE IF NOT EXISTS ddl_db_hive.ddl_student1
    as
    select
           id,name
    from
         default.student;

-- COMMENT '根据已经存在的表结构创建表'
USE ddl_db_hive;
CREATE TABLE IF NOT EXISTS ddl_student2 LIKE ddl_db_hive.ddl_student0;

-- COMMENT '显示数据库ddl_db_hive的表'
USE ddl_db_hive;
SHOW TABLES ;

-- COMMENT '分区表'
USE ddl_db_hive ;
CREATE TABLE IF NOT EXISTS ddl_dept_partition(
    dep_tno int,
    dep_name string,
    dep_loc string
)
PARTITIONED BY (month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;

-- COMMENT '加载数据'
load data local inpath '/export/testData/dept.txt' into table ddl_db_hive.ddl_dept_partition partition(month ='201909');
load data local inpath '/export/testData/dept.txt' into table ddl_db_hive.ddl_dept_partition partition(month ='201908');
load data local inpath '/export/testData/dept.txt' into table ddl_db_hive.ddl_dept_partition partition(month ='201907');
-- COMMENT '查询分区表数据'
SELECT * FROM ddl_dept_partition where month  = '201909';
-- COMMENT '多分区联合查询'
SELECT * FROM ddl_dept_partition where month  = '201909'
UNION ALL
SELECT * FROM ddl_dept_partition where month  = '201908'
UNION ALL
SELECT * FROM ddl_dept_partition where month  = '201907';

-- COMMENT '增加分区'
ALTER TABLE ddl_dept_partition ADD partition(month = '201906');
ALTER TABLE ddl_dept_partition ADD partition(month = '201905')partition(month = '201904');

-- COMMENT '删除分区'
ALTER TABLE ddl_dept_partition DROP partition(month = '201906');
ALTER TABLE ddl_dept_partition DROP partition(month = '201905'),partition(month = '201904');

-- COMMENT '查看分区表分区数'
SHOW partitions ddl_dept_partition;

-- COMMENT '查看分区表结构'
DESC FORMATTED ddl_dept_partition;

-- COMMENT '创建二级分区表'
USE ddl_db_hive;
CREATE TABLE IF NOT EXISTS ddl_dept_partition_2(
    dept_no int,
    dept_name string,
    dept_loc string
)
PARTITIONED BY(month string,day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- COMMENT '加载数据到分区表的方式一'
LOAD DATA LOCAL INPATH
    '/export/testData/dept.txt'
INTO TABLE
    ddl_db_hive.ddl_dept_partition_2
PARTITION(month = '201903',day = '01');

-- COMMENT '查询分区数据'
SELECT
    *
FROM
    ddl_dept_partition_2
WHERE
        month = '201903' AND day = '01' ;

desc formatted ddl_dept_partition_2;

-- COMMENT '把数据直接上传到分区目录上，让分区表和数据产生关联的三种方式'
-- 方式一，上传数据后修复
dfs -mkdir -p /db_hive/ddl_db_hive/ddl_dept_partition_2/month=201903/day=02;
dfs -put /export/testData/dept.txt /db_hive/ddl_db_hive/ddl_dept_partition_2/month=201903/day=02;
-- 执行修复命令
MSCK REPAIR TABLE ddl_dept_partition_2;
-- 查询数据
SELECT
       *
FROM
     ddl_dept_partition_2
WHERE
      month = '201903'
AND
      day = '02';

-- 方式二，上传数据后添加分区
dfs -mkdir -p /db_hive/ddl_db_hive/ddl_dept_partition_2/month=201903/day=03;
dfs -put /export/testData/dept.txt /db_hive/ddl_db_hive/ddl_dept_partition_2/month=201903/day=03;
-- 执行添加分区
ALTER TABLE
    ddl_dept_partition_2
    ADD PARTITION (month = '201903',day = '03');
-- 查询数据
SELECT
    *
FROM
    ddl_dept_partition_2
WHERE
        month = '201903'
  AND
        day = '03';

-- 方式三，创建文件夹后 load 数据到分区
-- 创建文件夹
dfs -mkdir -p /db_hive/ddl_db_hive/ddl_dept_partition_2/month=201903/day=04;
-- 上传数据 load
LOAD DATA LOCAL INPATH
    '/export/testData/dept.txt'
INTO TABLE
    ddl_dept_partition_2
    PARTITION (month = '201903',day = '04');
-- 查询数据
SELECT
    *
FROM
    ddl_dept_partition_2
WHERE
        month = '201903'
  AND
        day = '04';


show tables;
desc ddl_db_hive.ddl_dept_partition_2;
-- COMMENT '修改表'
-- 重命名表
ALTER TABLE ddl_db_hive.ddl_dept_partition
    RENAME TO ddl_dept_partition_1 ;
ALTER TABLE ddl_db_hive.ddl_dept_partition_1
    RENAME TO ddl_dept_partition ;
-- 增加或替换列
ALTER TABLE ddl_db_hive.ddl_dept_partition ADD COLUMNS (deptdesc int);
ALTER TABLE ddl_db_hive.ddl_dept_partition REPLACE COLUMNS (dept_no int,dept_name string,dept_loc string,dept_desc string);
-- 更新列
ALTER TABLE ddl_db_hive.ddl_dept_partition CHANGE COLUMN deptdesc dept_desc int;

-- COMMENT '删除表'
DROP TABLE table_name;





