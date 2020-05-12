-- https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select

-- COMMENT '查询语句语法'
[WITH CommonTableExpression (, CommonTableExpression)*]    (Note: Only available starting with Hive 0.13.0)
SELECT [ALL | DISTINCT]
    select_expression,select_expression,...
FROM
    table_reference
[WHERE where_condition]
[GROUP BY col_list]
[ORDER BY col_list]
[CLUSTER BY col_list
    | [DISTRIBUTE BY col_list]
      [SORT BY col_list]
]
[LIMIT number]
;


------------------------------------------------------------------------
------------------------------------------------------------------------
-- https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select

-- COMMENT '查询语句语法'
---------------------------------------------------------------------

-- 数据库和表搭建
---------------------------------------------------------------------
DESC DATABASE EXTENDED ddl_db_test;

CREATE DATABASE IF NOT EXISTS query_db_hive
LOCATION '/db_hive/query_db_hive';
USE query_db_hive;
SHOW TABLES;

CREATE TABLE IF NOT EXISTS q_dept(
    dept_no int,
    dept_name string,
    dept_loc int
)
COMMENT '数据查询：部门表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE ;

CREATE TABLE IF NOT EXISTS q_emp(
    emp_no int,
    emp_name string,
    job string,
    mgr int,
    hired_date string,
    sal double,
    comm double,
    dept_no int
)
COMMENT '数据查询：员工表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE ;

LOAD DATA LOCAL INPATH '/export/testData/dept.txt' INTO TABLE q_dept;
LOAD DATA LOCAL INPATH '/export/testData/emp.txt' INTO TABLE q_emp;


--------------------------------------------------------------------
-- COMMENT '基本查询'
-- 全表查询
SELECT * FROM q_dept;
SELECT * FROM q_emp;
-- 特定字段查询
SELECT
    emp_no,emp_name
FROM q_emp;
-- 列别名
SELECT
    emp_name AS name,
    dept_no as dn
FROM q_emp;
-- 算术运算符
SELECT
    sal + 1 AS plus_1_sal
FROM q_emp;
-- 常用函数
    -- count()
SELECT
    count(emp_no) count
FROM q_emp;
    --max()
SELECT
    max(sal) AS max_sal
FROM q_emp;
    -- min()
SELECT
    min(sal) AS min_sal
FROM q_emp;
    -- sum()
SELECT
    sum(sal) AS sum_sal
FROM q_emp;
    -- avg()
SELECT
    avg(sal) AS avg_sal
FROM q_emp;
    -- limit
SELECT * FROM q_emp LIMIT 5;

---------------------------------------------------------------------
-- COMMENT 'where查询'
SELECT avg(sal) avg_2000 FROM q_emp WHERE sal > 2000;
SELECT sum(sal) sum_3000 FROM q_emp WHERE sal = 3000;
SELECT count(sal) count_3000 FROM q_emp t WHERE sal <= 3000 ;
SELECT count(sal) count_3000 FROM q_emp t1,q_dept t2 WHERE t1.dept_no = t2.dept_no;
SELECT count(sal) count FROM q_emp;
SELECT count(sal) count FROM q_emp WHERE sal>3000;
SELECT count(sal) count FROM q_emp WHERE sal<=>3000;
SELECT count(sal) count FROM q_emp WHERE sal BETWEEN 500 AND 2000;
SELECT * FROM q_emp WHERE comm IS NULL;
SELECT * FROM q_emp WHERE sal IN (3000,5000);

SELECT * FROM q_emp WHERE saL LIKE '2%';
SELECT * FROM q_emp WHERE saL LIKE '_2%';
SELECT * FROM q_emp WHERE saL RLIKE '[2]';

SELECT * FROM q_emp WHERE sal > 1000 AND dept_no = 30;
SELECT * FROM q_emp WHERE sal > 1000 OR dept_no = 30;
SELECT * FROM q_emp WHERE sal > 1000
UNION ALL
SELECT * FROM q_emp WHERE dept_no = 30;

SELECT * FROM q_emp WHERE dept_no NOT IN (20,30);
SELECT
    t1.*
FROM
    q_emp AS t1
LEFT JOIN (
    SELECT t2.*
    FROM q_emp t2
    WHERE dept_no = 20 OR dept_no = 30
         ) t3
ON
    (t1.dept_no = t3.dept_no)
WHERE
    t3.dept_no IS NULL;
SELECT q_emp.* FROM q_emp
left join
    (SELECT q_emp.* FROM q_emp WHERE dept_no=20
    UNION
    SELECT q_emp.* FROM q_emp WHERE dept_no=30) t
on t.dept_no = q_emp.dept_no
where t.dept_no is null;

USE query_db_hive;
SHOW TABLES ;
show functions ;
---------------------------------------------------------------------
-- COMMENT '分组'
    -- group by
SELECT t.dept_no,avg(t.sal) avg_sal_dept
FROM q_emp t
GROUP BY t.dept_no;

SELECT t.dept_no,t.job,max(sal) max_sal_dept_job
FROM q_emp t
GROUP BY t.dept_no,t.job;
    -- having
SELECT t.dept_no,avg(sal) avg_sal_dept
FROM q_emp t
GROUP BY t.dept_no
HAVING avg_sal_dept >2000;

---------------------------------------------------------------------
-- COMMENT 'Join语句'


-- 求差集
USE query_db_hive;
CREATE TABLE IF NOT EXISTS q_reserve_order (
    id int COMMENT 'auto increment id',
    order_id int COMMENT 'the order id' ,
    cal_date string COMMENT 'order id create time'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS RCFILE ;
INSERT INTO
    q_reserve_order
VALUES
    (1, 100, '2018-07-05'),
    (2, 101, '2018-07-05'),
    (3, 102, '2018-07-05');

CREATE TABLE IF NOT EXISTS q_finish_order (
    id int COMMENT 'auto increment id',
    order_id int COMMENT 'the order id' ,
    cal_date string COMMENT 'order id finish time'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS RCFILE ;
INSERT INTO
    q_finish_order
VALUES
    (1, 100, '2018-07-05'),
    (3, 102, '2018-07-06'),
    (4, 103, '2018-07-05');
SELECT * FROM q_reserve_order;
SELECT * FROM q_finish_order;

-- 求下了单，但是还没完成的订单
    -- NOT IN 方式
SELECT reserve.*
FROM q_reserve_order AS reserve
WHERE
    reserve.order_id NOT IN (
            SELECT order_id
            FROM q_finish_order
        );
    -- LEFT OUTER JOIN 方式
SELECT
    reserve.*
FROM
    q_reserve_order AS reserve
LEFT OUTER JOIN
    q_finish_order AS finish
ON
    reserve.order_id = finish.order_id
WHERE
    finish.order_id IS NULL;



-- 求下了单，并且结单，但结单日期与下单日期不一致的订单
    -- NOT IN 方式：hive 值只支持单列的 NOT IN/IN 操作
SELECT reserve.*
FROM q_reserve_order AS reserve JOIN q_finish_order AS finish
ON (reserve.order_id = finish.order_id)
WHERE (reserve.order_id,reserve.cal_date) NOT IN (
    SELECT f.order_id,f.cal_date
    FROM q_finish_order AS f);
    -- LEFT OUTER JOIN 方式
SELECT
    reserve.*
FROM
    q_finish_order AS finish
LEFT OUTER JOIN
    q_reserve_order AS reserve
ON
    finish.order_id = reserve.order_id
WHERE
    finish.cal_date != reserve.cal_date;

use query_db_hive;
---------------------------------------------------------------------
-- COMMENT '排序'
SELECT * FROM q_emp ORDER BY sal;
SELECT * FROM q_emp ORDER BY sal ASC LIMIT 5;
SELECT * FROM q_emp ORDER BY sal DESC LIMIT 5;
SELECT emp_name,sal*2 ts FROM q_emp ORDER BY ts ASC LIMIT 5;
SELECT * FROM q_emp ORDER BY dept_no,sal LIMIT 5;

SET MAPREDUCE.JOB.REDUCES = 3;
SET MAPREDUCE.JOB.REDUCES ;
SELECT * FROM q_emp SORT BY dept_no DESC LIMIT 10;

set  hive.exec.dynamic.partition;

INSERT OVERWRITE LOCAL DIRECTORY '/export/testData/export/sort_by_result/q_emp/1'
SELECT * FROM q_emp SORT BY emp_name DESC ;
INSERT OVERWRITE  DIRECTORY '/user/yi/hive/export/sort_by_result/q_emp/2'
SELECT * FROM q_emp DISTRIBUTE BY dept_no SORT BY emp_name DESC ;
SELECT * FROM q_emp CLUSTER BY dept_no LIMIT 5;


---------------------------------------------------------------------
-- COMMENT '分桶、抽样查询'、client 不支持某些命令
USE query_db_hive;
    -- 分桶
CREATE TABLE IF NOT EXISTS q_student(
    id int,
    name string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE ;
LOAD DATA LOCAL INPATH '/export/testData/student.txt' INTO TABLE q_student;
SELECT * FROM q_student;
INSERT INTO TABLE q_student SELECT id,name FROM default.student;
DESC FORMATTED q_student;
INSERT INTO TABLE q_student SELECT id,name FROM default.student;
SELECT * FROM q_student;

CREATE TABLE IF NOT EXISTS q_student_buckets(
    id int,
    name string
)
CLUSTERED BY (id)
INTO 4 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE ;
DESC FORMATTED q_student_buckets;
LOAD DATA LOCAL INPATH '/export/testData/student.txt' INTO TABLE q_student_buckets;
SELECT * FROM q_student_buckets;
TRUNCATE TABLE q_student_buckets;
INSERT INTO q_student_buckets SELECT id,name FROM q_student;

SET HIVE.ENFORCE.BUCKETING = TRUE;
SET MAPREDUCE.JOB.REDUCES=-1;

    -- 分桶抽样查询
SELECT * FROM q_student_buckets TABLESAMPLE (BUCKET 1 OUT OF 2 ON id);
---------------------------------------------------------------------
-- COMMENT '常用查询函数'
-- 空字段赋值

SELECT comm, NVL(comm, -1) comm_nvl
FROM q_emp
LIMIT 5;
SELECT comm, NVL(comm, mgr) comm_nvl
FROM q_emp
LIMIT 5;

-- CASE WHEN
CREATE TABLE IF NOT EXISTS q_emp_sex
(
    name    string,
    dept_id string,
    sex     string
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '/export/testData/emp_sex.txt' INTO TABLE q_emp_sex;
SELECT *FROM q_emp_sex LIMIT 5;
-- 求出不同部门男女各多少人：
SELECT dept_id,
       sum(case sex when '男' then 1 else 0 end) male_count,
       sum(case sex when '女' then 1 else 0 end) femal_count
FROM q_emp_sex
GROUP BY dept_id;

use query_db_hive;
show tables;
-- 行转列
CREATE TABLE IF NOT EXISTS q_person_info
(
    name          string,
    constellation string,
    blood_type    string
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '/export/testData/constellation.txt' INTO TABLE q_person_info;
SELECT *FROM q_person_info LIMIT 5;
-- 按照星座和血型一致的人归类
SELECT
    t1.base,
    concat_ws("|",collect_set(t1.name)) persons
FROM (
         SELECT
                name,
                concat(constellation, ",", blood_type) base
         FROM q_person_info
     ) t1
GROUP BY t1.base;


-- 列转行
USE query_db_hive;
DROP TABLE IF EXISTS  q_movie_info;
CREATE TABLE IF NOT EXISTS q_movie_info(
    movie string,
    category array<string>
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
STORED AS textfile ;
LOAD DATA LOCAL INPATH '/export/testData/movie.txt' INTO TABLE q_movie_info;
SELECT * FROM q_movie_info LIMIT 5;
-- 将电影分类的数组数据展开
SELECT
    movie,
    category_name
FROM
    q_movie_info lateral view  explode(category) table_tmp as category_name;



--a:shandong,b:beijing,c:hebei|1,2,3,4,5,6,7,8,9|[{"source":"7fresh","monthSales":4900,"userCount":1900,"score":"9.9"},{"source":"jd","monthSales":2090,"userCount":78981,"score":"9.8"},{"source":"jdmart","monthSales":6987,"userCount":1600,"score":"9.0"}]
DROP TABLE IF EXISTS q_explode_lateral_view;
CREATE TABLE IF NOT EXISTS q_explode_lateral_view(
    area string,
    goods_id string,
    sale_info string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS textfile ;
TRUNCATE TABLE q_explode_lateral_view;
LOAD DATA LOCAL INPATH '/export/testData/goods_sale_info_area.txt' INTO TABLE q_explode_lateral_view;
SELECT * FROM q_explode_lateral_view LIMIT 5;
    -- 解析 array字段，goods_id
SELECT explode(split(goods_id,',')) as goods_id FROM q_explode_lateral_view;
    -- 解析 map字段，area
SELECT explode(split(area,',')) as area FROM q_explode_lateral_view;
    -- 拆解 json 字段 sale_info,把一个语句把单行数据拆解成多行后的数据结果集
SELECT goods_id_explode,sale_info
FROM q_explode_lateral_view
LATERAL VIEW EXPLODE(split(goods_id,',')) goods_tmp as goods_id_explode;
    -- 全拆解：列转行【3表的笛卡尔积：q_explode_lateral_view/虚表area_tmp/虚表goods_id_tmp】
SELECT
    area_lve,
    goods_id_lve,
    sale_info
FROM q_explode_lateral_view
LATERAL VIEW EXPLODE(split(area,',')) area_tmp as area_lve
LATERAL VIEW EXPLODE(split(goods_id,',')) goods_id_tmp as goods_id_lve;
    -- 从 sale_info 字段中找出所有的monthSales 并且行展示
SELECT
    --get_json_object(concat('{',sale_info_lve,'}'),'$.monthSales') as monthSales
    get_json_object(concat('{',sale_info_lve,'}'),'$') as monthSales
    --sale_info_lve
FROM q_explode_lateral_view
LATERAL VIEW EXPLODE(split(
    regexp_replace(
        regexp_replace(
            sale_info,'\\[\\{','')
                                ,'\\}\\]',''),'},\\{')) sale_info_tmp as sale_info_lve;
-- 将json格式的一行数据，完全转换成二维表的方式展现
SELECT
    area,
    goods_id,
    get_json_object(concat('{',sale_info_lve,'}'),'$.source') as source,
    get_json_object(concat('{',sale_info_lve,'}'),'$.monthSales') as monthSales,
    get_json_object(concat('{',sale_info_lve,'}'),'$.userCount') as userCount,
    get_json_object(concat('{',sale_info_lve,'}'),'$.score') as score
FROM
     q_explode_lateral_view
LATERAL VIEW EXPLODE(split(
    regexp_replace(
        regexp_replace(sale_info,'\\[\\{',''),
        '}]',''),'},\\{'))
    sale_info_tmp as sale_info_lve;


-- 窗口函数
USE query_db_hive;
DROP TABLE IF EXISTS q_window_business;
CREATE TABLE IF NOT EXISTS q_window_business(
    name string,
    order_date string,
    cost int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS textfile ;
LOAD DATA LOCAL INPATH '/export/testData/business.txt' INTO TABLE q_window_business;
SELECT * FROM q_window_business LIMIT 5;
-- 查询在2017年4月份购买过的顾客以及总人数
SELECT
    name,
    count(name) over()
FROM q_window_business
WHERE substring(order_date,1,7) = '2017-04'
GROUP BY name;
-- 查询顾客的购买明细以及月购买总额
SELECT
    name,
    order_date,
    cost,
    sum(cost) over(partition by month(order_date)) AS total_cost_month
FROM q_window_business
LIMIT 5;
-- 上述场景，将每个顾客的cost按照日期进行累加
SELECT name,
       -- 所有行相加
       sum(cost) over() AS ts,
       -- 按name分组，组内数据聚合--相加
       sum(cost) over(partition by name) AS ts_name,
       -- 按name分组，组内数据聚合--累加
       sum(cost) over(partition by name order by order_date) AS ts_name_by_date,
       -- 按name分组，组内数据聚合--累加
       sum(cost) over(partition by name order by order_date rows between UNBOUNDED PRECEDING AND CURRENT ROW ) AS ts_name_by_date_upcr,
       -- 按name分组，当前行和前一行聚合
       sum(cost) over(partition by name order by order_date rows between 1 PRECEDING AND CURRENT ROW) AS ts_name_1pur,
       -- 按name分组，当前行和前边一行以及后面一行聚合
       sum(cost) over(partition by name order by order_date rows between 1 PRECEDING AND 1 FOLLOWING) AS ts_name_1p1f,
       -- 按name分组，当前行以及后面所有行聚合
       sum(cost) over(partition by name order by order_date rows between CURRENT ROW AND UNBOUNDED FOLLOWING) AS ts_name_cruf
FROM q_window_business
LIMIT 5;

-- 查看顾客上次的购买时间
SELECT name,order_date,cost,
       lag(order_date,1,'1900-01-01') over (partition by name order by order_date) as time_1,
       lag(order_date,2) over (partition by name order by order_date) as time_2
FROM q_window_business
LIMIT 5;

-- 查询前20%时间的订单信息
SELECT
    name, order_date, cost
FROM (
     SELECT name,order_date,cost,
            ntile(5) over (order by order_date) sorted
     FROM q_window_business
         )t1
WHERE sorted = 1;

-- http://lxw1234.com/archives/category/hive/page/4


-- RANK
USE query_db_hive;
DROP TABLE IF EXISTS q_score;
CREATE TABLE IF NOT EXISTS q_score(
    name string,
    subject string,
    score int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE ;
LOAD DATA LOCAL INPATH '/export/testData/score.txt' INTO TABLE q_score;
SELECT * FROM q_score LIMIT 5;

-- 计算没门学科成绩排名
SELECT
    name,subject,score,
    rank() over (partition by subject order by score desc) rp
FROM q_score;

-- 求出没门学科前三的学生
SELECT
    *
FROM (
     SELECT name,subject,score,
            rank() over (partition by subject order by score desc) rp,
            dense_rank() over (partition by subject order by score desc) drp,
            row_number() over (partition by subject order by score desc) rnp
     FROM q_score
         ) t
WHERE t.rp<=3 AND subject='数学';
set hive.exec.mode.local.auto;