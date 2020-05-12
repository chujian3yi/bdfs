CREATE DATABASE IF NOT EXISTS f_db_hive
    LOCATION '/db_hive/f_db_hive';
SHOW DATABASES ;
USE f_db_hive;
SHOW TABLES ;
SET hive.exec.mode.local.auto;

---------------------------------------------
-- function                                --
---------------------------------------------
-- 系统内置函数
show functions ;
desc function array;
desc function extended count;

-- 自定义函数
-- UDF (user-defined functions):一进一出
-- UDAF (user-defined aggregation functions):聚集函数，多进一出。类似 count/max/min
-- UDTF (user-defined table-generating functions):一进多出，类似LATERAL VIEW EXPLORE()
-- 打包jar上传服务器：/export/testData/jars/yl.jar
-- 将jar包添加到hive的classpath
add jar /export/testData/jars/yl.jar;
-- 创建临时函数与开发好的 java class关联
create temporary function yi_lower as 'com.hadoop.yi.hive.udf.YiLower';
-- 使用
USE default;
select ename,yi_lower(ename) lower_name from emp;

-- UDAF
add jar /export/testData/jars/g_udaf.jar;
drop temporary function g_udaf_sum;
create temporary function g_udaf_sum as 'com.hadoop.yi.hive.udaf.GenericUdafMemberLevel2';
select g_udaf_sum(score,2.0) from student;

-- UDTF