import os
import re
import configparser
import mysql.connector
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from logging import getLogger, INFO, basicConfig
from contextlib import contextmanager
import jieba
from ltp import LTP
from textrank4zh import TextRank4Keyword

# networkx版本检测
try:
    import networkx as nx

    nx_version = tuple(map(int, nx.__version__.split(".")))
    if nx_version >= (3, 0):
        logger = getLogger(__name__)
        logger.warning(
            "networkx版本为3.x及以上，TextRank关键词提取将被禁用！建议pip install networkx==2.6.3以启用TextRank。")
        TEXTRANK_ENABLED = False
    else:
        TEXTRANK_ENABLED = True
except Exception as e:
    TEXTRANK_ENABLED = False
    logger = getLogger(__name__)
    logger.warning(f"networkx检测失败，TextRank关键词提取将被禁用: {e}")

# 日志配置
basicConfig(level=INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = getLogger(__name__)

# 配置管理
config = configparser.ConfigParser()
config_file_path = os.path.join(os.getcwd(), 'init', 'config.ini')
config.read(config_file_path)

# MySQL数据库配置
DB_CONFIG = {
    'host': '192.168.10.105',
    'user': 'licz.1',
    'password': 'GjFmT5NEiE',
    'database': 'sq_liufengdb',
    'charset': 'utf8mb4',
    'connect_timeout': 10
}

# 全局配置
MAX_WORKERS = 10
BATCH_SIZE = 5


@contextmanager
def db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        yield conn
    except mysql.connector.Error as e:
        logger.error(f"数据库连接失败: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            try:
                conn.close()
            except Exception as e:
                logger.warning(f"关闭数据库连接时出错: {str(e)}")


# ---------------- NLP画像提取核心 ----------------
class UserProfileExtractor:
    COLOR_WORDS = set(['红', '黄', '蓝', '绿', '紫', '粉', '黑', '白', '灰', '橙', '棕', '青', '金', '银', '褐', '米'])
    HOBBY_WORDS = set(
        ['唱歌', '跳舞', '游泳', '跑步', '篮球', '足球', '羽毛球', '乒乓球', '看书', '阅读', '写作', '绘画', '摄影',
         '旅游', '旅行', '美食', '烹饪', '下棋', '钓鱼', '游戏', '电影', '音乐', '动漫'])
    FOOD_WORDS = set(
        ['米饭', '面条', '包子', '饺子', '牛肉', '羊肉', '鸡肉', '鱼', '虾', '螃蟹', '苹果', '香蕉', '橙子', '西瓜',
         '葡萄', '辣', '甜', '咸', '酸', '苦', '辣椒', '火锅', '烧烤', '寿司', '披萨', '汉堡', '沙拉', '蛋糕',
         '巧克力'])

    EXTRACTION_CONFIG = {
        'favorite_foods': {'patterns': [r'(?:喜欢吃|爱吃|最爱吃|偏好吃|常吃|喜欢的食物是)([^。；，,\n]+)'],
                           'dict': FOOD_WORDS},
        'dietary_restrictions': {'patterns': [r'(?:不吃|忌口|过敏|不能吃|避免吃)([^。；，,\n]+)'], 'dict': FOOD_WORDS},
        'favorite_colors': {'patterns': [r'(?:喜欢|偏爱|最喜欢)([^。；，,\n]+?色)'], 'dict': COLOR_WORDS},
        'hobbies': {'patterns': [r'(?:爱好|兴趣|喜欢做的事是)([^。；，,\n]+)'], 'dict': HOBBY_WORDS},
        'favorite_games': {'patterns': [r'(?:喜欢玩|常玩|爱玩|沉迷于|推荐的游戏是)([^。；，,\n]+)'], 'dict': set()},
        'favorite_books': {'patterns': [r'(?:喜欢读|常读|爱读|推荐的书是|在看)([^。；，,\n]+)'], 'dict': set()},
        'favorite_movies': {'patterns': [r'(?:喜欢看|常看|爱看|推荐的电影是|在看)([^。；，,\n]+)'], 'dict': set()},
        'achievements': {'patterns': [r'(?:获得|取得|完成|实现|达成)([^。；，,\n]+?成就)'], 'dict': set()},
        'dreams': {'patterns': [r'(?:梦想是|我的梦想是|我想)([^。；，,\n]+)'], 'dict': set()},
        'ideals': {'patterns': [r'(?:理想是|我的理想是)([^。；，,\n]+)'], 'dict': set()},
        'plans': {'patterns': [r'(?:计划|打算|准备)([^。；，,\n]+)'], 'dict': set()},
        'goals': {'patterns': [r'(?:目标是|我的目标是|希望)([^。；，,\n]+)'], 'dict': set()},
        'thoughts': {'patterns': [r'(?:我认为|我觉得|我的看法是|我的观点是)([^。；，,\n]+)'], 'dict': set()},
        'inspirations': {'patterns': [r'(?:灵感来源于|受到启发|启发是)([^。；，,\n]+)'], 'dict': set()},
        'family_memories': {'patterns': [r'(?:家人|家庭)([^。；，,\n]+?回忆)'], 'dict': set()},
        'friend_memories': {'patterns': [r'(?:朋友)([^。；，,\n]+?回忆)'], 'dict': set()},
        'colleague_memories': {'patterns': [r'(?:同事)([^。；，,\n]+?回忆)'], 'dict': set()},
        'visited_places': {'patterns': [r'(?:去过|到过|曾经去过|旅游过)([^。；，,\n]+)'], 'dict': set()},
        'desired_travel_places': {'patterns': [r'(?:想去|希望去|计划去|梦想去)([^。；，,\n]+)'], 'dict': set()},
    }

    def __init__(self, use_ltp=True, use_textrank=True):
        jieba.initialize()
        self.ltp = None
        self.use_ltp = False
        if use_ltp and LTP is not None:
            try:
                self.ltp = LTP()
                self.ltp_has_seg = hasattr(self.ltp, 'seg')
                self.ltp_has_pipeline = hasattr(self.ltp, 'pipeline')
                if self.ltp_has_seg or self.ltp_has_pipeline:
                    self.use_ltp = True
                else:
                    logger.warning("LTP对象不包含seg或pipeline方法，将自动降级为jieba分词。")
            except Exception as e:
                logger.warning(f"LTP初始化失败，已降级为jieba: {e}")
        self.use_textrank = use_textrank and TEXTRANK_ENABLED

    def _extract_by_patterns(self, text: str, patterns: List[str], dict_words: set, field: str) -> List[str]:
        results = set()
        for pat in patterns:
            for match in re.findall(pat, text):
                tokens = []
                # LTP分词+NER优先
                if self.use_ltp:
                    try:
                        if getattr(self, 'ltp_has_seg', False):
                            seg, _ = self.ltp.seg([match])
                            tokens = seg[0]
                        elif getattr(self, 'ltp_has_pipeline', False):
                            pipe_res = self.ltp.pipeline([match], tasks=['cws', 'ner'])
                            tokens = pipe_res.cws[0]
                            # NER召回地名
                            if field in ['visited_places', 'desired_travel_places'] and hasattr(pipe_res, 'ner'):
                                for ent in pipe_res.ner[0]:
                                    if ent[1] == 'Ni':
                                        results.add(ent[0])
                    except Exception as e:
                        logger.warning(f"LTP分词/NER失败，降级为jieba: {e}")
                        tokens = list(jieba.cut(match))
                else:
                    tokens = list(jieba.cut(match))
                # 领域词典过滤
                if dict_words:
                    for w in tokens:
                        if w in dict_words:
                            results.add(w)
                else:
                    for w in tokens:
                        if w:
                            results.add(w)
                # TextRank关键词补充
                if self.use_textrank and field in ['achievements', 'dreams', 'plans', 'goals', 'thoughts']:
                    try:
                        tr4w = TextRank4Keyword()
                        tr4w.analyze(text=match, lower=True, window=2)
                        for kw in tr4w.get_keywords(5, word_min_len=1):
                            results.add(kw.word)
                    except Exception as e:
                        logger.warning(f"TextRank关键词提取失败: {e}")
        return self._post_process_field(list(results), field)

    def analyze_interaction(self, query: str, response: str) -> Dict[str, Any]:
        full_text = f"用户: {query}\n回复: {response}"
        extracted = {}
        for field, conf in self.EXTRACTION_CONFIG.items():
            values = self._extract_by_patterns(full_text, conf['patterns'], conf['dict'], field)
            if values:
                extracted[field] = values
        # 冲突去重
        if 'favorite_foods' in extracted and 'dietary_restrictions' in extracted:
            favs = set(extracted['favorite_foods'])
            res = set(extracted['dietary_restrictions'])
            extracted['favorite_foods'] = sorted(favs - res)
        if 'visited_places' in extracted and 'desired_travel_places' in extracted:
            visited = set(extracted['visited_places'])
            desired = set(extracted['desired_travel_places'])
            extracted['desired_travel_places'] = sorted(desired - visited)
        return extracted

    def _post_process_field(self, values: List[str], field: str) -> List[str]:
        # 统一去重、排序、格式化
        unique = set()
        for v in values:
            v = v.strip()
            if v and v not in unique:
                unique.add(v)
        return sorted(unique)


# ---------------- 数据库与批量处理 ----------------
def get_user_basic_info(user_id: str) -> Optional[Dict[str, Any]]:
    try:
        with db_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("""
                    SELECT DISTINCT user_id, user_name
                    FROM ods_user_interaction_di
                    WHERE user_id = %s
                """, (user_id,))
                # print(cursor.fetchone())
                return cursor.fetchone()
    except Exception as e:
        logger.error(f"获取用户 {user_id} 基本信息时出错: {e}")
        return None


def get_user_interactions(user_id: str) -> List[Dict[str, Any]]:
    try:
        with db_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("""
                    SELECT query_text, response_text, interaction_time
                    FROM ods_user_interaction_di
                    WHERE user_id = %s
                    ORDER BY interaction_time
                """, (user_id,))
                # print(cursor.fetchall())
                return cursor.fetchall()
    except Exception as e:
        logger.error(f"获取用户 {user_id} 交互记录时出错: {e}")
        return []


def initialize_user_profile(user_id: str, user_name: str) -> Dict[str, Any]:
    return {
        'user_id': user_id,
        'stat_date': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
        'user_name': user_name,
        'favorite_foods': None,
        'dietary_restrictions': None,
        'favorite_colors': None,
        'hobbies': None,
        'favorite_games': None,
        'favorite_books': None,
        'favorite_movies': None,
        'achievements': None,
        'dreams': None,
        'ideals': None,
        'plans': None,
        'goals': None,
        'thoughts': None,
        'inspirations': None,
        'family_memories': None,
        'friend_memories': None,
        'colleague_memories': None,
        'visited_places': None,
        'desired_travel_places': None
    }


def merge_values(current: Optional[str], new: Optional[List[str]], separator: str = ', ') -> Optional[str]:
    if not new: return current
    if not current: return separator.join(new)
    current_set = set([x.strip() for x in current.split(separator) if x.strip()])
    new_set = set([x.strip() for x in new if x.strip()])
    merged = current_set | new_set
    return separator.join(sorted(merged)) if merged else None


def analyze_user_interactions(user_id: str, extractor: UserProfileExtractor) -> Optional[Dict[str, Any]]:
    user_info = get_user_basic_info(user_id)
    if not user_info:
        logger.warning(f"未找到用户 {user_id} 的基本信息")
        return None
    profile = initialize_user_profile(user_info['user_id'], user_info['user_name'])
    interactions = get_user_interactions(user_id)
    if not interactions:
        logger.info(f"用户 {user_id} 没有交互记录")
        return profile
    for interaction in interactions:
        query = interaction['query_text']
        response = interaction['response_text']
        analysis = extractor.analyze_interaction(query, response)
        profile['favorite_foods'] = merge_values(profile['favorite_foods'], analysis.get('favorite_foods'))
        profile['dietary_restrictions'] = merge_values(profile['dietary_restrictions'],
                                                       analysis.get('dietary_restrictions'))
        profile['favorite_colors'] = merge_values(profile['favorite_colors'], analysis.get('favorite_colors'))
        profile['hobbies'] = merge_values(profile['hobbies'], analysis.get('hobbies'))
        profile['favorite_games'] = merge_values(profile['favorite_games'], analysis.get('favorite_games'))
        profile['favorite_books'] = merge_values(profile['favorite_books'], analysis.get('favorite_books'))
        profile['favorite_movies'] = merge_values(profile['favorite_movies'], analysis.get('favorite_movies'))
        profile['achievements'] = merge_values(profile['achievements'], analysis.get('achievements'))
        profile['dreams'] = merge_values(profile['dreams'], analysis.get('dreams'))
        profile['ideals'] = merge_values(profile['ideals'], analysis.get('ideals'))
        profile['plans'] = merge_values(profile['plans'], analysis.get('plans'))
        profile['goals'] = merge_values(profile['goals'], analysis.get('goals'))
        profile['thoughts'] = merge_values(profile['thoughts'], analysis.get('thoughts'))
        profile['inspirations'] = merge_values(profile['inspirations'], analysis.get('inspirations'))
        profile['family_memories'] = merge_values(profile['family_memories'], analysis.get('family_memories'))
        profile['friend_memories'] = merge_values(profile['friend_memories'], analysis.get('friend_memories'))
        profile['colleague_memories'] = merge_values(profile['colleague_memories'], analysis.get('colleague_memories'))
        profile['visited_places'] = merge_values(profile['visited_places'], analysis.get('visited_places'))
        profile['desired_travel_places'] = merge_values(profile['desired_travel_places'],
                                                        analysis.get('desired_travel_places'))
    return profile


def convert_value_to_db(value: Any) -> Optional[Any]:
    if isinstance(value, str):
        value = value.strip()
        if value == "None" or value == '':
            return None
    return value


def save_profiles_batch(profiles: List[Dict[str, Any]]):
    if not profiles:
        return 0
    fields = [
        'user_id', 'stat_date', 'user_name', 'favorite_foods', 'dietary_restrictions', 'favorite_colors',
        'hobbies', 'favorite_games', 'favorite_books', 'favorite_movies',
        'achievements', 'dreams', 'ideals', 'plans', 'goals',
        'thoughts', 'inspirations', 'family_memories', 'friend_memories', 'colleague_memories',
        'visited_places', 'desired_travel_places'
    ]
    fields_str = ', '.join(f'`{f}`' for f in fields)
    # print(fields_str)
    values_str = ', '.join(['%s'] * len(fields))
    update_fields = [f for f in fields if f not in ['user_id', 'stat_date']]
    # print(update_fields)
    update_str = ', '.join([f'`{f}`=%s' for f in update_fields])
    print(update_str)
    user_stat_pairs = [(p['user_id'], p['stat_date']) for p in profiles]
    print(profiles)
    print(user_stat_pairs)
    exist_set = set()
    try:
        with db_connection() as conn:
            with conn.cursor() as cursor:
                format_strings = ','.join(['(%s,%s)'] * len(user_stat_pairs))
                print(format_strings)
                if user_stat_pairs:
                    print(
                        f"SELECT user_id, stat_date FROM dwd_user_comprehensive_profile_di_0625 WHERE (user_id, stat_date) IN ({format_strings})")
                    print([v for pair in user_stat_pairs for v in pair])
                    cursor.execute(
                        f"SELECT user_id, stat_date FROM dwd_user_comprehensive_profile_di_0625 WHERE (user_id, stat_date) IN ({format_strings})",
                        [v for pair in user_stat_pairs for v in pair])
                    # exist_set = set(cursor.fetchall())
                    exist_set = set((row[0], str(row[1])) for row in cursor.fetchall())
                    print(f'exist_set: {exist_set}')
    except Exception as e:
        logger.warning(f"查询已存在用户画像时出错，跳过去重: {e}")
    insert_rows, update_rows = [], []
    for profile in profiles:
        key = (profile['user_id'], profile['stat_date'])
        print(f'key: {key}')
        row = [convert_value_to_db(profile.get(f)) for f in fields]
        if key in exist_set:
            update_rows.append([convert_value_to_db(profile.get(f)) for f in update_fields] + [profile['user_id'],
                                                                                               profile['stat_date']])
            print(f'update_rows: {update_rows}')
        else:
            insert_rows.append(tuple(row))
            print(f'insert_rows: {insert_rows}')
    count = 0
    try:
        with db_connection() as conn:
            with conn.cursor() as cursor:
                if insert_rows:
                    insert_sql = f"INSERT INTO dwd_user_comprehensive_profile_di_0625 ({fields_str}, created_time, updated_time) VALUES ({values_str}, NOW(), NOW())"
                    cursor.executemany(insert_sql, insert_rows)
                    count += cursor.rowcount
                if update_rows:
                    update_sql = f"UPDATE dwd_user_comprehensive_profile_di_0625 SET {update_str}, updated_time=NOW() WHERE user_id=%s AND stat_date=%s"
                    cursor.executemany(update_sql, update_rows)
                    count += cursor.rowcount
                conn.commit()
                return count
    except Exception as e:
        logger.error(f"批量保存用户画像时出错: {e}", exc_info=True)
        if 'conn' in locals():
            conn.rollback()
        return 0


def get_all_user_ids() -> List[str]:
    try:
        with db_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT DISTINCT user_id FROM ods_user_interaction_di ORDER BY user_id DESC")
                return [row['user_id'] for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"获取用户ID列表时出错: {e}")
        return []


def process_all_users():
    start_time = time.time()
    logger.info("开始处理用户画像...")
    extractor = UserProfileExtractor()
    user_ids = get_all_user_ids()
    if not user_ids:
        logger.info("没有找到需要处理的用户")
        return
    logger.info(f"找到 {len(user_ids)} 个需要处理的用户")
    profiles = []
    saved_count = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_userid = {executor.submit(analyze_user_interactions, user_id, extractor): user_id for user_id in
                            user_ids}
        for i, future in enumerate(as_completed(future_to_userid)):
            user_id = future_to_userid[future]
            try:
                profile = future.result()
                if profile:
                    profiles.append(profile)
                    logger.info(f"成功处理用户 {user_id}")
                else:
                    logger.info(f"用户 {user_id} 没有有效数据")
            except Exception as e:
                logger.error(f"处理用户 {user_id} 时发生错误: {e}")
            # 每BATCH_SIZE个批量保存一次
            if len(profiles) >= BATCH_SIZE:
                batch = profiles[:BATCH_SIZE]
                count = save_profiles_batch(batch)
                saved_count += count
                logger.info(f"已保存/更新 {count} 个用户画像 (累计: {saved_count})")
                profiles = profiles[BATCH_SIZE:]
        # 保存剩余未满BATCH_SIZE的
        if profiles:
            count = save_profiles_batch(profiles)
            saved_count += count
            logger.info(f"已保存/更新 {count} 个用户画像 (累计: {saved_count})")
    logger.info(f"用户画像数据生成完成，总耗时: {time.time() - start_time:.2f}秒")


if __name__ == "__main__":
    process_all_users()
