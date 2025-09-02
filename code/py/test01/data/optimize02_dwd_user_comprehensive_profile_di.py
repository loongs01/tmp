import os
import json
import re
import configparser
import mysql.connector
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Set, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from transformers import pipeline
from functools import wraps
import logging
from dataclasses import dataclass
from contextlib import contextmanager
import psutil

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('user_profile_analysis.log')
    ]
)
logger = logging.getLogger(__name__)

# 初始化情感分析模型
try:
    sentiment_pipeline = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")
except Exception as e:
    logger.warning(f"无法加载情感分析模型: {e}")
    sentiment_pipeline = None

# 配置管理
config = configparser.ConfigParser()
config_file_path = os.path.join(os.path.dirname(os.getcwd()), 'init', 'config.ini')
config.read(config_file_path)

DEEPSEEK_API_KEY = config.get('deepseek', 'deepseek.api_key', fallback="")
DEEPSEEK_ENDPOINT = config.get('deepseek', 'deepseek.endpoint', fallback="https://api.deepseek.com/v1/chat/completions")
DEEPSEEK_MODEL = config.get('deepseek', 'deepseek.model', fallback="deepseek-chat")

# MySQL数据库配置
DB_CONFIG = {
    'host': '192.168.10.105',
    'user': 'licz.1',
    'password': 'GjFmT5NEiE',
    'database': 'sq_liufengdb',
    'charset': 'utf8mb4',
    'pool_name': 'user_profile_pool',
    'pool_size': 10,
    'connect_timeout': 10
}

# 全局配置
MAX_WORKERS = 10
BATCH_SIZE = 100
API_RETRY_TIMES = 3
API_TIMEOUT = 30
DB_OPERATION_RETRY = 3


# 数据模型
@dataclass
class UserProfile:
    user_id: str
    stat_date: str
    user_name: Optional[str] = None
    favorite_foods: Optional[str] = None
    dietary_restrictions: Optional[str] = None
    favorite_colors: Optional[str] = None
    hobbies: Optional[str] = None
    favorite_games: Optional[str] = None
    favorite_books: Optional[str] = None
    favorite_movies: Optional[str] = None
    achievements: Optional[str] = None
    dreams: Optional[str] = None
    ideals: Optional[str] = None
    plans: Optional[str] = None
    goals: Optional[str] = None
    thoughts: Optional[str] = None
    inspirations: Optional[str] = None
    family_memories: Optional[str] = None
    friend_memories: Optional[str] = None
    colleague_memories: Optional[str] = None
    visited_places: Optional[str] = None
    desired_travel_places: Optional[str] = None


class DatabaseConnectionError(Exception):
    """自定义数据库连接错误"""
    pass


def log_memory_usage():
    """记录内存使用情况"""
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    logger.debug(
        f"内存使用: RSS={mem_info.rss / 1024 / 1024:.2f}MB, "
        f"VMS={mem_info.vms / 1024 / 1024:.2f}MB"
    )


def retry(max_retries: int = 3, delay: float = 1, exceptions=(Exception,)):
    """重试装饰器"""

    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        sleep_time = delay * (2 ** attempt)  # 指数退避
                        logger.warning(f"尝试 {attempt + 1}/{max_retries} 失败: {e}. 等待 {sleep_time:.1f}秒后重试...")
                        time.sleep(sleep_time)
            raise last_exception if last_exception else Exception("未知错误")

        return wrapper

    return decorator


class DatabaseManager:
    """数据库连接管理"""
    _pool = None

    @classmethod
    def get_connection(cls):
        if cls._pool is None:
            try:
                cls._pool = mysql.connector.pooling.MySQLConnectionPool(**DB_CONFIG)
                logger.info("数据库连接池创建成功")
            except mysql.connector.Error as e:
                raise DatabaseConnectionError(f"数据库连接池创建失败: {e}")

        try:
            return cls._pool.get_connection()
        except mysql.connector.Error as e:
            raise DatabaseConnectionError(f"从连接池获取连接失败: {e}")

    @classmethod
    def close_pool(cls):
        if cls._pool:
            # 兼容不同版本的MySQL Connector/Python
            if hasattr(cls._pool, '_remove_connections'):
                cls._pool._remove_connections()
            logger.info("数据库连接池已关闭")


class UserProfileExtractor:
    def __init__(self, api_key: str = None, endpoint: str = None, model: str = None):
        """初始化用户画像提取器"""
        self.api_key = api_key or DEEPSEEK_API_KEY
        self.endpoint = endpoint or DEEPSEEK_ENDPOINT
        self.model = model or DEEPSEEK_MODEL

        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        self.session = requests.Session()
        self.session.mount('https://', requests.adapters.HTTPAdapter(max_retries=3))

    def _construct_prompt(self, query: str, response: str) -> str:
        """构建全面的提示词"""
        prompt_template = """
        你是一个专业的用户画像分析师，擅长从对话中提取详细的个人信息。
        请从以下聊天记录中提取以下信息，如果某个信息不存在，请返回None。
        格式为JSON对象：
        {{
            "饮食偏好": {{
                "favorite_foods": "逗号分隔的食物列表",
                "dietary_restrictions": "逗号分隔的饮食限制"
            }},
            "颜色偏好": {{
                "favorite_colors": "逗号分隔的颜色列表"
            }},
            "兴趣爱好": {{
                "hobbies": "逗号分隔的爱好列表",
                "favorite_games": "逗号分隔的游戏列表",
                "favorite_books": "逗号分隔的书籍列表",
                "favorite_movies": "逗号分隔的电影列表"
            }},
            "个人发展": {{
                "achievements": "用户成就描述",
                "dreams": "用户梦想描述",
                "ideals": "用户理想描述",
                "plans": "用户计划描述",
                "goals": "用户目标描述"
            }},
            "想法与灵感": {{
                "thoughts": "用户想法描述",
                "inspirations": "用户灵感描述"
            }},
            "记忆": {{
                "family_memories": "家人记忆描述",
                "friend_memories": "朋友记忆描述",
                "colleague_memories": "同事记忆描述"
            }},
            "旅行信息": {{
                "visited_places": "逗号分隔的已访问地点列表",
                "desired_travel_places": "逗号分隔的想去地点列表"
            }}
        }}

        聊天记录：
        用户询问: {query}
        回复内容: {response}
        """
        return prompt_template.format(query=query, response=response)

    @retry(max_retries=API_RETRY_TIMES, delay=1, exceptions=(requests.RequestException,))
    def _call_deepseek_api(self, prompt: str) -> Dict[str, Any]:
        """调用DeepSeek API"""
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3,
            "max_tokens": 1000
        }

        try:
            response = self.session.post(
                self.endpoint,
                headers=self.headers,
                data=json.dumps(payload),
                timeout=(API_TIMEOUT, API_TIMEOUT)  # 连接和读取超时
            )
            response.raise_for_status()

            if response.status_code != 200:
                raise requests.HTTPError(f"API返回非200状态码: {response.status_code}")

            return response.json()
        except requests.Timeout:
            logger.error("API调用超时")
            raise
        except requests.RequestException as e:
            logger.error(f"API请求失败: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"API响应JSON解析失败: {e}")
            raise

    def _parse_api_response(self, api_response: Dict[str, Any]) -> Dict[str, Any]:
        """解析API响应"""
        default_response = {
            "饮食偏好": {"favorite_foods": None, "dietary_restrictions": None},
            "颜色偏好": {"favorite_colors": None},
            "兴趣爱好": {
                "hobbies": None,
                "favorite_games": None,
                "favorite_books": None,
                "favorite_movies": None
            },
            "个人发展": {
                "achievements": None,
                "dreams": None,
                "ideals": None,
                "plans": None,
                "goals": None
            },
            "想法与灵感": {"thoughts": None, "inspirations": None},
            "记忆": {
                "family_memories": None,
                "friend_memories": None,
                "colleague_memories": None
            },
            "旅行信息": {"visited_places": None, "desired_travel_places": None}
        }

        try:
            if "error" in api_response:
                logger.error(f"API返回错误: {api_response['error']}")
                return default_response

            content = api_response["choices"][0]["message"]["content"]
            json_match = re.search(r'```jsons*(.*?)s*```', content, re.DOTALL)
            if not json_match:
                logger.warning("无法从响应中提取JSON内容")
                return default_response

            json_str = json_match.group(1)
            parsed_data = json.loads(json_str)

            # 深度合并默认响应和解析数据
            result = default_response.copy()
            for category, category_data in parsed_data.items():
                if category in result and isinstance(category_data, dict):
                    result[category].update(
                        {k: v for k, v in category_data.items() if k in result[category]}
                    )

            return result
        except Exception as e:
            logger.error(f"解析响应失败: {e}")
            return default_response

    def analyze_interaction(self, query: str, response: str) -> Dict[str, Any]:
        """分析单条交互记录"""
        try:
            prompt = self._construct_prompt(query, response)
            api_response = self._call_deepseek_api(prompt)
            return self._parse_api_response(api_response)
        except Exception as e:
            logger.error(f"分析交互记录失败: {e}")
            return self._parse_api_response({"error": str(e)})


def merge_values(current: Optional[str], new: Optional[str], separator: str = ', ') -> Optional[str]:
    """合并两个值，处理None情况"""
    if isinstance(current, str):
        current = current.strip()
        if current.lower() == "none" or not current:
            current = None
    if isinstance(new, str):
        new = new.strip()
        if new.lower() == "none" or not new:
            new = None

    if not current and not new:
        return None
    if not current:
        return new
    if not new:
        return current

    # 去重合并
    current_set = set(item.strip() for item in current.split(separator) if item.strip())
    new_set = set(item.strip() for item in new.split(separator) if item.strip())
    merged = current_set.union(new_set)
    return separator.join(sorted(merged)) if merged else None


def get_user_ids() -> List[str]:
    """获取所有需要处理的用户ID"""
    try:
        with DatabaseManager.get_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT DISTINCT user_id FROM ods_user_interaction_di ORDER BY user_id DESC")
                return [row['user_id'] for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"获取用户ID列表失败: {e}")
        return []


def analyze_user_interactions(user_id: str, extractor: UserProfileExtractor) -> Optional[UserProfile]:
    """分析单个用户的交互数据并生成画像"""
    try:
        with DatabaseManager.get_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                # 获取用户基本信息
                cursor.execute("""
                               SELECT DISTINCT user_id, user_name
                               FROM ods_user_interaction_di
                               WHERE user_id = %s
                               """, (user_id,))
                user_info = cursor.fetchone()

                if not user_info:
                    logger.warning(f"未找到用户 {user_id} 的基本信息")
                    return None

                # 初始化用户画像
                profile = UserProfile(
                    user_id=user_id,
                    stat_date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                    user_name=user_info.get('user_name')
                )

                # 查询用户所有交互记录
                cursor.execute("""
                               SELECT query_text, response_text
                               FROM ods_user_interaction_di
                               WHERE user_id = %s
                               ORDER BY interaction_time
                               """, (user_id,))
                interactions = cursor.fetchall()

                if not interactions:
                    logger.info(f"用户 {user_id} 没有交互记录")
                    return profile

                # 处理每条交互记录
                for interaction in interactions:
                    analysis = extractor.analyze_interaction(
                        interaction['query_text'],
                        interaction['response_text']
                    )

                    # 更新用户画像
                    profile.favorite_foods = merge_values(
                        profile.favorite_foods,
                        analysis['饮食偏好']['favorite_foods']
                    )
                    profile.dietary_restrictions = merge_values(
                        profile.dietary_restrictions,
                        analysis['饮食偏好']['dietary_restrictions']
                    )
                    profile.favorite_colors = merge_values(
                        profile.favorite_colors,
                        analysis['颜色偏好']['favorite_colors']
                    )
                    profile.hobbies = merge_values(
                        profile.hobbies,
                        analysis['兴趣爱好']['hobbies']
                    )
                    profile.favorite_games = merge_values(
                        profile.favorite_games,
                        analysis['兴趣爱好']['favorite_games']
                    )
                    profile.favorite_books = merge_values(
                        profile.favorite_books,
                        analysis['兴趣爱好']['favorite_books']
                    )
                    profile.favorite_movies = merge_values(
                        profile.favorite_movies,
                        analysis['兴趣爱好']['favorite_movies']
                    )
                    profile.achievements = merge_values(
                        profile.achievements,
                        analysis['个人发展']['achievements']
                    )
                    profile.dreams = merge_values(
                        profile.dreams,
                        analysis['个人发展']['dreams']
                    )
                    profile.ideals = merge_values(
                        profile.ideals,
                        analysis['个人发展']['ideals']
                    )
                    profile.plans = merge_values(
                        profile.plans,
                        analysis['个人发展']['plans']
                    )
                    profile.goals = merge_values(
                        profile.goals,
                        analysis['个人发展']['goals']
                    )
                    profile.thoughts = merge_values(
                        profile.thoughts,
                        analysis['想法与灵感']['thoughts']
                    )
                    profile.inspirations = merge_values(
                        profile.inspirations,
                        analysis['想法与灵感']['inspirations']
                    )
                    profile.family_memories = merge_values(
                        profile.family_memories,
                        analysis['记忆']['family_memories']
                    )
                    profile.friend_memories = merge_values(
                        profile.friend_memories,
                        analysis['记忆']['friend_memories']
                    )
                    profile.colleague_memories = merge_values(
                        profile.colleague_memories,
                        analysis['记忆']['colleague_memories']
                    )
                    profile.visited_places = merge_values(
                        profile.visited_places,
                        analysis['旅行信息']['visited_places']
                    )
                    profile.desired_travel_places = merge_values(
                        profile.desired_travel_places,
                        analysis['旅行信息']['desired_travel_places']
                    )

                return profile
    except Exception as e:
        logger.error(f"处理用户 {user_id} 时发生错误: {e}")
        return None


def save_profile_batch(profiles: List[UserProfile]) -> None:
    """批量保存用户画像到数据库"""
    if not profiles:
        return

    try:
        with DatabaseManager.get_connection() as conn:
            with conn.cursor() as cursor:
                # 准备批量插入/更新语句
                update_sql = """
                             UPDATE dwd_user_comprehensive_profile_di_0625
                             SET user_name             = %s,
                                 favorite_foods        = %s,
                                 dietary_restrictions  = %s,
                                 favorite_colors       = %s,
                                 hobbies               = %s,
                                 favorite_games        = %s,
                                 favorite_books        = %s,
                                 favorite_movies       = %s,
                                 achievements          = %s,
                                 dreams                = %s,
                                 ideals                = %s,
                                 plans                 = %s,
                                 goals                 = %s,
                                 thoughts              = %s,
                                 inspirations          = %s,
                                 family_memories       = %s,
                                 friend_memories       = %s,
                                 colleague_memories    = %s,
                                 visited_places        = %s,
                                 desired_travel_places = %s,
                                 updated_time          = NOW()
                             WHERE user_id = %s 
                               AND stat_date = %s 
                             """

                insert_sql = """
                             INSERT INTO dwd_user_comprehensive_profile_di_0625 (user_id, stat_date, user_name, 
                                                                                 favorite_foods, dietary_restrictions, 
                                                                                 favorite_colors, hobbies, 
                                                                                 favorite_games, favorite_books, 
                                                                                 favorite_movies, 
                                                                                 achievements, dreams, ideals, plans, 
                                                                                 goals, thoughts, inspirations, 
                                                                                 family_memories, friend_memories, 
                                                                                 colleague_memories, visited_places, 
                                                                                 desired_travel_places, created_time, 
                                                                                 updated_time) 
                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                     %s, NOW(), NOW()) 
                             """

                # 批量处理
                for profile in profiles:
                    # 检查记录是否存在
                    cursor.execute("""
                                   SELECT 1
                                   FROM dwd_user_comprehensive_profile_di_0625
                                   WHERE user_id = %s
                                     AND stat_date = %s
                                   """, (profile.user_id, profile.stat_date))

                    params = (
                        profile.user_name,
                        profile.favorite_foods,
                        profile.dietary_restrictions,
                        profile.favorite_colors,
                        profile.hobbies,
                        profile.favorite_games,
                        profile.favorite_books,
                        profile.favorite_movies,
                        profile.achievements,
                        profile.dreams,
                        profile.ideals,
                        profile.plans,
                        profile.goals,
                        profile.thoughts,
                        profile.inspirations,
                        profile.family_memories,
                        profile.friend_memories,
                        profile.colleague_memories,
                        profile.visited_places,
                        profile.desired_travel_places,
                        profile.user_id,
                        profile.stat_date
                    )

                    if cursor.fetchone():
                        cursor.execute(update_sql, params)
                    else:
                        cursor.execute(insert_sql, (
                            profile.user_id,
                            profile.stat_date,
                            *params[:-2]  # 去掉最后的user_id和stat_date
                        ))

                conn.commit()
                logger.info(f"成功保存 {len(profiles)} 个用户画像")
    except Exception as e:
        logger.error(f"批量保存用户画像失败: {e}")
        if 'conn' in locals():
            conn.rollback()


def process_user_batch(user_ids: List[str], extractor: UserProfileExtractor) -> List[UserProfile]:
    """处理一批用户"""
    profiles = []
    # 动态调整线程数
    workers = min(MAX_WORKERS, len(user_ids), (os.cpu_count() or 1) * 2)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_user = {
            executor.submit(analyze_user_interactions, user_id, extractor): user_id
            for user_id in user_ids
        }

        for future in as_completed(future_to_user):
            user_id = future_to_user[future]
            try:
                profile = future.result()
                if profile:
                    profiles.append(profile)
                    logger.info(f"成功处理用户 {user_id}")
                else:
                    logger.warning(f"用户 {user_id} 没有有效数据")
            except Exception as e:
                logger.error(f"处理用户 {user_id} 时发生错误: {e}")

            # 定期记录内存使用情况
            if len(profiles) % 5 == 0:
                log_memory_usage()

    return profiles


def process_all_users() -> None:
    """处理所有有交互记录的用户"""
    logger.info("开始处理用户画像...")
    start_time = time.time()
    processed_users = 0
    failed_users = 0

    try:
        # 初始化提取器
        extractor = UserProfileExtractor()

        # 获取所有用户ID
        user_ids = get_user_ids()
        if not user_ids:
            logger.warning("没有找到需要处理的用户")
            return

        logger.info(f"找到 {len(user_ids)} 个需要处理的用户")

        # 分批处理用户
        for i in range(0, len(user_ids), BATCH_SIZE):
            batch_ids = user_ids[i:i + BATCH_SIZE]
            logger.info(
                f"处理批次 {i // BATCH_SIZE + 1}/{(len(user_ids) - 1) // BATCH_SIZE + 1} "
                f"(用户数: {len(batch_ids)})"
            )

            # 处理当前批次
            profiles = process_user_batch(batch_ids, extractor)
            processed_users += len(profiles)
            failed_users += (len(batch_ids) - len(profiles))

            # 保存当前批次结果
            if profiles:
                save_profile_batch(profiles)
            else:
                logger.info("本批次没有需要保存的用户画像")

        total_time = time.time() - start_time
        logger.info(
            f"用户画像处理完成，总耗时: {total_time:.2f}秒n"
            f"处理用户数: {processed_users}n"
            f"失败用户数: {failed_users}n"
            f"平均处理速度: {processed_users / max(total_time, 0.1):.2f} 用户/秒"
        )
    except Exception as e:
        logger.error(f"处理用户画像时发生严重错误: {e}")
    finally:
        try:
            DatabaseManager.close_pool()
        except Exception as e:
            logger.error(f"关闭数据库连接池时出错: {e}")


if __name__ == "__main__":
    process_all_users()
