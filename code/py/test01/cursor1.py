import os
import re
import configparser
import mysql.connector
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Union, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from logging import getLogger, INFO, basicConfig
from transformers import pipeline
from contextlib import contextmanager
from tenacity import retry, stop_after_attempt, wait_exponential
from collections import defaultdict
import jieba
import jieba.posseg as pseg
import unicodedata

# --- 全局配置 ---
basicConfig(level=INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = getLogger(__name__)

# 环境与缓存配置
os.environ["TRANSFORMERS_CACHE"] = "D:/transformers_cache"
os.environ["TRANSFORMERS_OFFLINE"] = "1"
os.environ["TRANSFORMERS_NO_ADVISE"] = "1"

# 数据库配置
DB_CONFIG = {
    'host': '192.168.10.105',
    'user': 'licz.1',
    'password': 'GjFmT5NEiE',
    'database': 'sq_liufengdb',
    'charset': 'utf8mb4',
    'connect_timeout': 10
}

# 业务配置
MAX_WORKERS = 10
BATCH_SIZE = 100
SEMANTIC_SIM_THRESHOLD = 0.85

# --- 静态资源与词典 (扩展后) ---
DOMAIN_DICTS = {
    "food": {"口味": ["辣", "甜", "咸", "酸", "麻辣"], "中餐": ["川菜", "粤菜", "湘菜"],
             "食材": ["猪肉", "牛肉", "鸡肉", "海鲜"]},
    "hobby": {"游戏": ["PlayStation", "PC游戏", "手游"], "阅读": ["小说", "科幻", "历史"],
              "电影": ["动作片", "喜剧", "科幻片"]},
    "color": {"基本色": ["红色", "蓝色", "黄色", "绿色", "黑色", "白色", "紫色", "橙色", "灰色"]},
    "travel": {"国内": ["北京", "上海", "云南", "西藏"], "国外": ["日本", "美国", "欧洲"]},
    "personal_development": {"领域": ["事业", "学业", "家庭", "健康"]}
}

# 预编译正则表达式
EMOJI_PATTERN = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
CONTROL_CHAR_PATTERN = re.compile(r'[\x00-\x1F\x7F]')
ALLOWED_CHAR_PATTERN = re.compile(r"[^a-zA-Z0-9\u4e00-\u9fff，。！？、；：'""（）《》【】,.!?;:'\"()<>\[\]{}\-_=+@#$%^&*/\\|~`\s]")


# --- 核心辅助函数 ---
def levenshtein(a: str, b: str) -> float:
    if a == b: return 1.0
    if not a or not b: return 0.0
    len_a, len_b = len(a), len(b)
    if abs(len_a - len_b) > max(len_a, len_b) * (1 - SEMANTIC_SIM_THRESHOLD): return 0.0
    dp = list(range(len_b + 1))
    for i in range(1, len_a + 1):
        prev = dp[0]
        dp[0] = i
        for j in range(1, len_b + 1):
            temp = dp[j]
            cost = 0 if a[i - 1] == b[j - 1] else 1
            dp[j] = min(dp[j - 1] + 1, prev + 1, prev + cost)
            prev = temp
    dist = dp[len_b]
    return 1 - dist / max(len_a, len_b, 1)


# --- 数据库与NLP初始化 ---
class DatabaseConnectionError(Exception): pass


@contextmanager
def db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        yield conn
    except mysql.connector.Error as e:
        raise DatabaseConnectionError(f"数据库连接失败: {str(e)}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            conn.close()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def init_nlp_pipelines():
    try:
        return (
            pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english"),
            pipeline("question-answering", model="deepset/bert-base-cased-squad2")
        )
    except Exception as e:
        logger.critical(f"NLP管道初始化失败: {e}", exc_info=True)
        raise


# --- 核心数据清洗与提取类 (全面重构) ---
class UserProfileExtractor:
    _jieba_initialized = False
    _sentiment_pipeline, _qa_pipeline = None, None

    # --- 全字段提取配置中心 ---
    EXTRACTION_CONFIG = {
        # 饮食偏好
        'favorite_foods': {'domain': 'food',
                           'patterns': [r'(?:喜欢|爱吃|偏好)(?:的|是)?\s*([^。，,；;、\n]+?)(?:菜|食物)?'],
                           'mode': 'noun_phrase'},
        'dietary_restrictions': {'domain': 'food',
                                 'patterns': [r'(?:不吃|不能|忌口|对)(?:\s*是)?\s*([^。，,；;、\n]+?)(?:过敏)?'],
                                 'mode': 'noun_phrase'},
        # 颜色偏好
        'favorite_colors': {'domain': 'color', 'patterns': [r'(?:喜欢|偏爱)的?颜色是(.+?)', r'喜欢(.+?色)'],
                            'mode': 'domain_dict'},
        # 兴趣爱好 (细分)
        'hobbies': {'domain': 'hobby',
                    'patterns': [r'我的爱好是(.+)', r'(?:平常|业余时间)?喜欢(.+?)[。，,]', r'对(.+?)感兴趣'],
                    'mode': 'noun_phrase'},
        'favorite_games': {'domain': 'hobby',
                           'patterns': [r'(?:喜欢玩|在玩|推荐|沉迷于)\s*《?([^》\n,]+?)》?(?:这款游戏)?'],
                           'mode': 'noun_phrase'},
        'favorite_books': {'domain': 'hobby', 'patterns': [r'(?:喜欢读|在看|推荐的书是)\s*《?([^》\n,]+?)》?'],
                           'mode': 'noun_phrase'},
        'favorite_movies': {'domain': 'hobby', 'patterns': [r'(?:喜欢看|推荐的电影是)\s*《?([^》\n,]+?)》?'],
                            'mode': 'noun_phrase'},
        # 个人发展
        'achievements': {'patterns': [r'(?:我|最近)?(?:完成|达成|实现|获得)了?\s*([^,。]+?)(?:的成就|的目标)?'],
                         'mode': 'raw'},
        'dreams': {'patterns': [r'(?:我的|我人生的)梦想是\s*([^,。]+)'], 'mode': 'raw'},
        'ideals': {'patterns': [r'我的理想是\s*([^,。]+)'], 'mode': 'raw'},
        'plans': {'patterns': [r'(?:我计划|打算|准备)(?![去旅行])\s*([^,。]+)'], 'mode': 'raw'},
        'goals': {'patterns': [r'(?:我的|今年的|短期)目标是\s*([^,。]+)'], 'mode': 'raw'},
        # 想法与灵感
        'thoughts': {'patterns': [r'(?:我觉得|我認為|我感觉)\s*([^,。]+)'], 'mode': 'raw'},
        'inspirations': {'patterns': [r'给了我.*?灵感', r'灵感来源于\s*([^,。]+)'], 'mode': 'raw'},
        # 记忆
        'family_memories': {'patterns': [r'(?:和|跟)家人\s*([^,。]+)的(?:回忆|事)'], 'mode': 'raw'},
        'friend_memories': {'patterns': [r'记得和朋友\s*([^,。]+)'], 'mode': 'raw'},
        'colleague_memories': {'patterns': [r'和同事一起\s*([^,。]+)'], 'mode': 'raw'},
        # 旅行信息
        'visited_places': {'domain': 'travel', 'patterns': [r'去过(?!.+旅行)([^,。]+)', r'曾经在(.+?)旅行'],
                           'mode': 'noun_phrase'},
        'desired_travel_places': {'domain': 'travel', 'patterns': [r'(?:想|计划|打算)去(.+?)旅行', r'想去的地方是(.+)'],
                                  'mode': 'noun_phrase'},
    }

    def __init__(self):
        self._init_lazy_resources()
        self._build_domain_maps()

    @classmethod
    def _init_lazy_resources(cls):
        if not cls._jieba_initialized:
            jieba.initialize()
            custom_dict_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'custom_dict.txt')
            if os.path.exists(custom_dict_path):
                jieba.load_userdict(custom_dict_path)
            cls._jieba_initialized = True
        if cls._sentiment_pipeline is None:
            cls._sentiment_pipeline, cls._qa_pipeline = init_nlp_pipelines()

    def _build_domain_maps(self):
        self.domain_terms = defaultdict(set)
        for domain, categories in DOMAIN_DICTS.items():
            for terms in categories.values():
                self.domain_terms[domain].update(terms)

    @staticmethod
    def _normalize_text(text: str) -> str:
        if not text: return ""
        text = unicodedata.normalize('NFKC', text)
        text = CONTROL_CHAR_PATTERN.sub('', text)
        text = EMOJI_PATTERN.sub('', text)
        text = ALLOWED_CHAR_PATTERN.sub('', text)
        return re.sub(r'\s+', ' ', text).strip().lower()

    def _post_process(self, items: List[str], config: Dict[str, Any]) -> List[str]:
        if not items: return []
        processed = set()
        mode = config.get('mode', 'noun_phrase')
        domain = config.get('domain')

        if mode == 'domain_dict' and domain:
            domain_specific_terms = self.domain_terms.get(domain, set())
            for item in items:
                normalized_item = self._normalize_text(item)
                for term in domain_specific_terms:
                    if term in normalized_item:
                        processed.add(term)
        elif mode == 'noun_phrase':
            for item in items:
                normalized_item = self._normalize_text(item)
                quoted = re.findall(r'[《"]([^》"]+)[》"]', normalized_item)
                if quoted:
                    processed.update(q for q in quoted if self.is_meaningful(q))
                    continue
                words = pseg.cut(normalized_item)
                for word, flag in words:
                    if flag.startswith('n') and len(word) > 1 and self.is_meaningful(word):
                        processed.add(word)
        else:  # mode == 'raw'
            for item in items:
                clean_item = self._normalize_text(item)
                if self.is_meaningful(clean_item):
                    processed.add(clean_item)

        return list(processed)

    def _extract_field(self, text: str, field_config: Dict[str, Any]) -> List[str]:
        extracted_items = set()
        for pattern in field_config.get('patterns', []):
            try:
                matches = re.findall(pattern, text)
                for match in matches:
                    item = " ".join(match) if isinstance(match, tuple) else match
                    extracted_items.add(item)
            except re.error as e:
                logger.warning(f"正则表达式错误: {pattern}, {e}")
        return self._post_process(list(extracted_items), field_config)

    def analyze_interaction(self, query: str, response: str) -> Dict[str, Any]:
        full_text = self._normalize_text(f"用户: {query} \n回复: {response}")
        extracted_data = {}
        for field, config in self.EXTRACTION_CONFIG.items():
            extracted_values = self._extract_field(full_text, config)
            if extracted_values:
                extracted_data[field] = extracted_values

        # 跨字段逻辑冲突处理
        if 'favorite_foods' in extracted_data and 'dietary_restrictions' in extracted_data:
            favs = set(extracted_data['favorite_foods'])
            res = set(extracted_data['dietary_restrictions'])
            conflicts = favs.intersection(res)
            if conflicts:
                extracted_data['favorite_foods'] = list(favs - conflicts)

        if 'visited_places' in extracted_data and 'desired_travel_places' in extracted_data:
            visited = set(extracted_data['visited_places'])
            desired = set(extracted_data['desired_travel_places'])
            conflicts = visited.intersection(desired)
            if conflicts:
                extracted_data['desired_travel_places'] = list(desired - conflicts)

        return {k: v for k, v in extracted_data.items() if v}

    @staticmethod
    def is_meaningful(text: str) -> bool:
        text_lower = text.strip().lower()
        if not text_lower: return False
        if text_lower in {"none", "null", "无", "没有", "不详", "未知", "test", "测试", "一些", "一种", "那个",
                          "这个"}: return False
        return not (len(text_lower) < 2 and not text_lower.isdigit())

    @staticmethod
    def semantic_similarity(a: str, b: str) -> float:
        return max(UserProfileExtractor.jaccard_similarity(a, b), levenshtein(a, b))

    @staticmethod
    def jaccard_similarity(a: str, b: str) -> float:
        set_a, set_b = set(a), set(b)
        if not set_a and not set_b: return 1.0
        if not set_a or not set_b: return 0.0
        return len(set_a & set_b) / len(set_a | set_b)

    @staticmethod
    def merge_values(current: Optional[Any], new: Optional[Any], separator: str = ', ') -> Optional[Any]:
        if not new: return current

        def to_list(val):
            if val is None: return []
            if isinstance(val, list): return [str(v).strip() for v in val if UserProfileExtractor.is_meaningful(str(v))]
            if isinstance(val, str): return [v.strip() for v in val.split(separator) if
                                             UserProfileExtractor.is_meaningful(v)]
            return [str(val).strip()] if UserProfileExtractor.is_meaningful(str(val)) else []

        current_list = to_list(current)
        new_list = to_list(new)

        for item in new_list:
            if not any(UserProfileExtractor.semantic_similarity(item, exist) > SEMANTIC_SIM_THRESHOLD for exist in
                       current_list):
                current_list.append(item)

        if not current_list: return None
        return current_list


# --- 数据库与业务流程 ---
def get_user_info_and_interactions(user_id: str) -> Tuple[Optional[Dict], List[Dict]]:
    try:
        with db_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT DISTINCT user_name FROM ods_user_interaction_di WHERE user_id = %s", (user_id,))
                user_info = cursor.fetchone()
                if not user_info: return None, []
                cursor.execute(
                    "SELECT query_text, response_text FROM ods_user_interaction_di WHERE user_id = %s ORDER BY interaction_time",
                    (user_id,))
                interactions = cursor.fetchall()
                return user_info, interactions
    except DatabaseConnectionError as e:
        logger.error(f"数据库连接错误: {e}")
    except Exception as e:
        logger.error(f"获取用户 {user_id} 数据时出错: {e}")
    return None, []


def analyze_user(user_id: str, extractor: UserProfileExtractor) -> Optional[Dict[str, Any]]:
    user_info, interactions = get_user_info_and_interactions(user_id)
    if not user_info:
        logger.warning(f"未找到用户 {user_id} 的基本信息或无交互")
        return None

    profile_fields = list(UserProfileExtractor.EXTRACTION_CONFIG.keys())
    profile = {
        'user_id': user_id,
        'stat_date': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
        'user_name': user_info['user_name'],
        **{field: None for field in profile_fields}
    }

    for interaction in interactions:
        analysis = extractor.analyze_interaction(interaction['query_text'], interaction['response_text'])
        for key, value in analysis.items():
            if key in profile:
                profile[key] = UserProfileExtractor.merge_values(profile[key], value)

    return profile


def convert_value_to_db(value: Any) -> Optional[str]:
    if value is None: return None
    if isinstance(value, list):
        items = sorted(list(set(str(item).strip() for item in value if item and str(item).strip())))
        return ", ".join(items) if items else None
    val_str = str(value).strip()
    return val_str if val_str and val_str.lower() != 'none' else None


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def save_profiles_batch(profiles: List[Dict[str, Any]]) -> int:
    if not profiles: return 0

    # 动态构建SQL以适应所有字段
    all_fields = set()
    for p in profiles:
        all_fields.update(p.keys())

    # 确保基础字段存在，并排序以保证一致性
    base_fields = ['user_id', 'stat_date', 'user_name']
    other_fields = sorted([f for f in all_fields if f not in base_fields and f not in ['created_time', 'updated_time']])
    fields = base_fields + other_fields

    fields_str = ", ".join(f"`{f}`" for f in fields)
    values_str = ", ".join(["%s"] * len(fields))
    updates_str = ", ".join([f"`{f}`=VALUES(`{f}`)" for f in other_fields])
    updates_str += ", `updated_time`=NOW()"

    sql = f"""
        INSERT INTO dwd_user_comprehensive_profile_di_0627 ({fields_str}) 
        VALUES ({values_str})
        ON DUPLICATE KEY UPDATE {updates_str}
    """

    data_to_insert = []
    for profile in profiles:
        row = []
        for field in fields:
            value = profile.get(field)
            row.append(convert_value_to_db(value))
        data_to_insert.append(tuple(row))

    try:
        with db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, data_to_insert)
                conn.commit()
                return cursor.rowcount
    except Exception as e:
        logger.error(f"批量保存用户画像时出错: {e}", exc_info=True)
        if 'conn' in locals() and conn.is_connected():
            conn.rollback()
        raise
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


def process_all_users() -> None:
    start_time = time.time()
    logger.info("开始处理用户画像...")
    try:
        extractor = UserProfileExtractor()
    except Exception:
        return

    user_ids = get_all_user_ids()
    if not user_ids:
        logger.info("没有找到需要处理的用户")
        return

    logger.info(f"找到 {len(user_ids)} 个需要处理的用户")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_user = {executor.submit(analyze_user, user_id, extractor): user_id for user_id in user_ids}

        profiles_to_save = []
        processed_count = 0
        for future in as_completed(future_to_user):
            processed_count += 1
            try:
                profile = future.result()
                if profile:
                    # 过滤掉完全没有提取到任何信息的profile
                    if any(profile.get(key) for key in UserProfileExtractor.EXTRACTION_CONFIG.keys()):
                        profiles_to_save.append(profile)
            except Exception as e:
                user_id = future_to_user[future]
                logger.error(f"处理用户 {user_id} 时发生致命错误: {e}", exc_info=True)

            if len(profiles_to_save) >= BATCH_SIZE or (processed_count == len(user_ids) and profiles_to_save):
                logger.info(f"达到批处理大小或处理完成，正在保存 {len(profiles_to_save)} 个画像...")
                try:
                    saved_count = save_profiles_batch(profiles_to_save)
                    logger.info(f"成功保存/更新 {saved_count} 行记录。")
                except Exception as e:
                    logger.error(f"批处理保存失败: {e}")
                finally:
                    profiles_to_save.clear()

    logger.info(f"用户画像数据生成完成，总耗时: {time.time() - start_time:.2f}秒")


if __name__ == "__main__":
    process_all_users()
