---mongodb
库：projectv_played


---mongodb测试库建表语句
-- 表名
db.createCollection("story_play_statistic");

--索引
db.getCollection("story_play_statistic").createIndex({
    etl_date: Int32("1"),
    lang: Int32("1"),
    status: Int32("1"),
    story_play_cnt: Int32("1")
}, {
    name: "idx_etl_date_lang_cnt",
    background: true
});

db.getCollection("story_play_statistic").createIndex({
    etl_date: Int32("1"),
    lang: Int32("1"),
    status: Int32("1"),
    story_total_duration: Int32("1")
}, {
    name: "idx_etl_date_lang_duration",
    background: true
});



播放量数值规则及翻拍作品新增字段需求，需在mongodb生产projectv_played库新增表：story_play_init，建表语句如下：
-- 建表
db.createCollection("story_play_init");
-- 索引
db.getCollection("story_play_init").createIndex({
    "name_en_short": NumberInt("1"),
    "analysis_date": NumberInt("-1")
}, {
    name: "idx_date_lang"
});



推荐作品板块兜底展示top18投放消耗最高作品需求，需在mongodb生产projectv_played库新增表：reelshort_book_level_info，建表语句如下：
-- 建表
db.createCollection("reelshort_book_level_info");
-- 索引
db.getCollection("reelshort_book_level_info").createIndex({
    "analysis_date": NumberInt("-1")
    "id": NumberInt("1")
    "lang": NumberInt("1")
}, {
    name: "idx_date_id_lang"
});





-- drama
-- 表名
db.createCollection("maxdrama_story_play_statistic");

--索引
db.getCollection("maxdrama_story_play_statistic").createIndex({
    etl_date: Int32("1"),
    lang: Int32("1"),
    status: Int32("1"),
    story_play_cnt: Int32("1")
}, {
    name: "idx_etl_date_lang_cnt",
    background: true
});


-- 建表
db.createCollection("user_video_exposure");

-- 建表
db.createCollection("user_watched_videos");


-- 建表
db.createCollection("maxdrama_story_play_init");
-- 索引
db.getCollection("maxdrama_story_play_init").createIndex({
    "name_en_short": NumberInt("1"),
    "analysis_date": NumberInt("-1")
}, {
    name: "idx_date_lang"
});



-- 建表
db.createCollection("dim_t99_adjust_ad_info");

-- 建表
db.createCollection("reelshort_book_level_info");


