-- 将 jar包上传
-- 执行etl
-- bin/yarn jar ~/export/testData/gl.jar com.hadoop.yi.hive.glyy.GlETLDriver /glv/input/video
show databases;
use default;
show tables;

-- # 上传原始数据，为etl 做准备
dfs -mkdir /glv/input/video;
dfs -mkdir /glv/input/user;
dfs -put /export/testData/video /glv/input/video;
dfs -put /export/testData/user.txt /glv/input/user;
-- # 执行jar包
-- hadoop jar gl.jar /glv/input/video /glv/output/video/etl_20200510

CREATE DATABASE IF NOT EXISTS gl_db_hive LOCATION '/db_hive/gl_db_hive';

-- 创建 ori表【外部表】，将数据跟hive表关联
USE gl_db_hive;
DROP TABLE IF EXISTS gl_video_ori;
CREATE EXTERNAL TABLE IF NOT EXISTS gl_video_ori
(
    videoId   string,
    uploader  string,
    age       int,
    category  array<string>,
    length    int,
    views     int,
    rate      float,
    ratings   int,
    comments  int,
    relatedId array<string>
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        COLLECTION ITEMS TERMINATED BY '&'
    LOCATION '/glv/output/video/etl_20200510';



DROP TABLE IF EXISTS gl_user_ori;
CREATE EXTERNAL TABLE IF NOT EXISTS gl_user_ori
(
    uploader string,
    videos   int,
    friends  int
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        COLLECTION ITEMS TERMINATED BY '&'
    LOCATION '/glv/input/user';
SELECT *
FROM gl_user_ori
LIMIT 5;

alter table gl_user_ori
    set tblproperties ('EXTERNAL' = 'TRUE');
DESC formatted gl_user_ori;

-- 创建orc表【内部表】，将数据加载到hive表
USE gl_db_hive;
show tables;

DROP TABLE IF EXISTS gl_video_orc;
CREATE TABLE IF NOT EXISTS gl_video_orc
(
    videoId   string,
    uploader  string,
    age       int,
    category  array<string>,
    length    int,
    views     int,
    rate      float,
    ratings   int,
    comments  int,
    relatedId array<string>
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        COLLECTION ITEMS TERMINATED BY '&'
    STORED AS ORC;

DROP TABLE IF EXISTS gl_user_orc;
CREATE TABLE IF NOT EXISTS gl_user_orc
(
    uploader string,
    videos   int,
    friends  int
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        COLLECTION ITEMS TERMINATED BY '&'
    STORED AS ORC;

SET hive.exec.mode.local.auto = true;
truncate table gl_video_orc;

-- 加载数据，从ori表到orc表
INSERT OVERWRITE TABLE gl_user_orc
SELECT *
FROM gl_user_ori;
SELECT *
FROM gl_user_orc
LIMIT 5;
INSERT OVERWRITE TABLE gl_video_orc
SELECT *
FROM gl_video_ori;
SELECT *
FROM gl_video_orc
LIMIT 5;

show functions;
--统计视频观看数Top10
-- 1.查询gl_video_orc表，根据观看数views全局排降序，取前十
-- 2.gl_video_orc,order by views limit 10
SELECT videoId,
       uploader,
       age,
       category,
       length,
       `views`,
       rate,
       ratings,
       comments,
       relatedId
FROM gl_video_orc
ORDER BY views
    DESC
LIMIT 10;

--统计视频类别热度Top10
-- 1.需求是：统计所有视频类别的视频热度，根据视频热度按照视频类别排序前十
-- 2.按照类别 group by聚合,(对类别做列转行展开);count组内的videoId个数作为热度;按照热度排序，取10条
-- 3.列转行展开：虚拟表tmp、gl_video_orc => t1，取展开后类别及类别视频=>t1；按照展开后类别分组聚合，取展开后类别视频计数做热度，降序排序取10

SELECT t1.category_name  AS category,
       count(t1.videoId) AS hot
FROM (SELECT videoId,
             category_name
      FROM gl_video_orc
               LATERAL VIEW EXPLODE(category) tmp_category AS category_name) t1
GROUP BY category_name
ORDER BY hot
    DESC
LIMIT 10;

--统计出视频观看数最高的20个视频的所属类别以及类别包含Top20视频的个数
-- 1.需求是：求top20观看数视频的类别及该类别下top20视频个数；
-- 2.从gl_video_orc求所有类别下的top20作t1；
-- 3.从t1中取视频和展开后类别作t2；
-- 4.从t2对展开后类别group by聚合，对组内视频计数作热度；根据热度降序排序

SELECT t2.category_name  AS category,
       count(t2.videoId) AS hot_with_views
FROM (SELECT videoId,
             category_name
      FROM (
               SELECT *
               FROM gl_video_orc
               ORDER BY views
                   DESC
               LIMIT 20
           ) t1
               LATERAL VIEW EXPLODE(category) tmp_category AS category_name) t2
GROUP BY category_name
ORDER BY hot_with_views DESC;

--统计视频观看数Top50所关联视频的所属类别Rank
-- 1.需求：视频观看数top50的每一个视频的关联视频的所属类别排序
-- 2.从 gl_video_orc中，取top50的视频的所有信息，作为t1
-- 3.从t1对关联视频relatedId列转行，取videoId，作为t2
-- 4.相关视频 t2 inner join gl_video_orc t3，取t3.category 和 t2.videoId(去重)，作为t4
-- 5.从t4对category 列转行category_name，取t4.videoId,category_name,作为t5
-- 6.从t5对 category_name做聚合，组内对videoId做计数，根据计数排行

SELECT category_name     AS category,
       count(t5.videoId) AS hot
FROM (SELECT category_name,
             videoId
      FROM (SELECT DISTINCT(t2.videoId),
                           t3.category
            FROM (SELECT explode(relatedId) AS videoId
                  FROM (SELECT *
                        FROM gl_video_orc
                        ORDER BY views
                            DESC
                        LIMIT 50) t1) t2
                     INNER JOIN gl_video_ori t3
                                ON gl_video_ori.videoId = t2.videoId) t4 LATERAL VIEW EXPLODE(category) tmp_category AS category_name) t5
GROUP BY category_name
ORDER BY hot
    DESC;

--统计每个类别中的视频热度Top10,以 Music为例
-- 1. 频繁的列转行于性能不好，创建视频类别展开表gl_category_orc 内部表（category -> categoryId）
-- 2. 插入数据，从表 gl_video_orc 查询得到
-- 3. 查询表 gl_category_orc ，按照 ratings 排序

USE gl_db_hive;
DROP TABLE IF EXISTS gl_category_orc;
CREATE EXTERNAL TABLE IF NOT EXISTS gl_category_orc
(
    videoId    string,
    uploader   string,
    age        int,
    categoryId string,
    length     int,
    visits     int,
    rate       float,
    ratings    int,
    comments   int,
    relatedId  array<string>
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        COLLECTION ITEMS TERMINATED BY '&'
    STORED AS ORC;

INSERT INTO TABLE gl_category_orc
SELECT videoId,
       uploader,
       age,
       category,
       length,
       `views`,
       rate,
       ratings,
       comments,
       relatedId
FROM gl_video_orc LATERAL VIEW EXPLODE(category) tmp_category AS categoryId;
SELECT *
FROM gl_category_orc
LIMIT 5;
SELECT COUNT(*)
FROM gl_category_orc;

SELECT videoId,
       uploader,
       age,
       categoryId,
       length,
       visits,
       rate,
       ratings,
       comments,
       relatedId
FROM gl_category_orc
WHERE categoryId = 'Music'
ORDER BY visits
    DESC
LIMIT 10;

-- 统计每个类别的视频热度top10，Music为例
FROM gl_video_orc
         LATERAL VIEW
             explode(category) tmp_c AS category_name
SELECT videoId,
       views,
       category_name
WHERE category_name = 'Music'
ORDER BY views DESC
LIMIT 10;

--统计每个类别中视频流量Top10，Music为例
-- 1.从类别展开表gl_category_orc查，根据ratings排序，取10
-- 2.或者从gl_video_orc查，展开category，根据ratings排序，
--1
FROM gl_category_orc
SELECT videoId,
       categoryId,
       ratings
WHERE categoryId = 'Music'
ORDER BY ratings DESC
LIMIT 10;
--2
FROM (
         FROM gl_video_orc
                  LATERAL VIEW
                      explode(category) tmp_c AS category_name
         SELECT videoId,
                ratings,
                category_name) t1
SELECT videoId,
       ratings,
       category_name
WHERE category_name = 'Music'
ORDER BY ratings DESC
LIMIT 10;

-- 统计每个类别中视频流量top10

FROM (
         FROM (
                  FROM gl_category_orc
                  SELECT videoId,
                         ratings,
                         categoryId) T1
         SELECT videoId,
                ratings,
                categoryId,
                RANK() OVER (PARTITION BY categoryId ORDER BY ratings DESC) rat) T2
SELECT videoId,
       ratings,
       categoryId,
       rat
WHERE rat <= 10;


--统计上传视频最多的用户Top10以及他们上传的视频

FROM (
         FROM gl_user_orc
         SELECT uploader,
                videos
         ORDER BY videos DESC
         LIMIT 10) T1
         JOIN
     gl_video_orc glv
     ON
         T1.uploader = glv.uploader
SELECT videoId,
       views
ORDER BY views DESC
LIMIT 20;

-- 统计上传视频最多的用户top10，他们每个人上传的视频rank
FROM (
         FROM (
                  FROM gl_user_orc
                  SELECT uploader,
                         videos
                  ORDER BY videos DESC
                  LIMIT 10) t1
                  JOIN gl_video_orc glv
                       ON t1.uploader = glv.uploader
         SELECT glv.videoId,
                glv.views,
                t1.uploader,
                RANK() OVER (PARTITION BY t1.uploader ORDER BY glv.views DESC) rp) t2
SELECT videoId,
       uploader,
       views
WHERE rp <= 20;


--统计每个类别视频观看数Top10

FROM (
         FROM gl_category_orc
         SELECT videoId,
                visits,
                categoryId,
                rank() over (partition by categoryId order by visits desc) rp) t1
SELECT videoId,
       visits,
       categoryId,
       rp
WHERE rp <= 10;

-- 统计所有类别的视频热度top10
FROM (
         FROM (
                  FROM gl_video_orc
                           LATERAL VIEW
                               explode(category) tmp_c AS category_name
                  SELECT videoId,
                         views,
                         category_name) t1
         SELECT videoId,
                views,
                category_name,
                RANK() OVER (PARTITION BY category_name ORDER BY views) hot) t2
SELECT videoId,
       category_name,
       hot
WHERE hot <= 10;