import os
import json
import re
import configparser
import mysql.connector
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging
from transformers import pipeline
from contextlib import contextmanager

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 配置读取
config = configparser.ConfigParser()
config_file_path = os.path.join(os.path.dirname(os.getcwd()), 'init', 'config.ini')
if not os.path.exists(config_file_path):
    logger.warning(f"配置文件未找到: {config_file_path}")
config.read(config_file_path)

DEEPSEEK_API_KEY = config.get('deepseek', 'deepseek.api_key', fallback="")
DEEPSEEK_ENDPOINT = config.get('deepseek', 'deepseek.endpoint', fallback="https://api.deepseek.com/v1/chat/completions")
DEEPSEEK_MODEL = config.get('deepseek', 'deepseek.model', fallback="deepseek-chat")

# MySQL数据库配置
DB_CONFIG = {
    'host': config.get('database', 'db.host', fallback='192.168.10.105'),
    'user': config.get('database', 'db.user', fallback='licz.1'),
    'password': config.get('database', 'db.password', fallback='GjFmT5NEiE'),
    'database': config.get('database', 'db.database', fallback='sq_liufengdb'),
    'charset': config.get('database', 'db.charset', fallback='utf8mb4'),
    'connect_timeout': config.getint('database', 'db.connect_timeout', fallback=10)
}

# 全局配置
MAX_WORKERS = 10
BATCH_SIZE = 5
API_RETRY_TIMES = 3

# 初始化情感分析管道
try:
    sentiment_pipeline = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")
except Exception as e:
    logger.warning(f"情感分析管道初始化失败: {str(e)}")
    sentiment_pipeline = None


class DatabaseConnectionError(Exception):
    """自定义数据库连接错误"""
    pass


@contextmanager
def db_connection():
    """数据库连接上下文管理器"""
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        logger.debug("数据库连接成功")
        yield conn
    except mysql.connector.Error as e:
        raise DatabaseConnectionError(f"数据库连接失败: {str(e)}")
    finally:
        if conn:
            try:
                conn.close()
                logger.debug("数据库连接已关闭")
            except Exception as e:
                logger.warning(f"关闭数据库连接时出错: {str(e)}")


class UserProfileExtractor:
    """用户画像提取器，使用DeepSeek API分析对话数据"""
    def __init__(self, api_key: Optional[str] = None, endpoint: Optional[str] = None, model: Optional[str] = None):
        self.api_key = api_key or DEEPSEEK_API_KEY
        self.endpoint = endpoint or DEEPSEEK_ENDPOINT
        self.model = model or DEEPSEEK_MODEL
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        self.session = requests.Session()

    def _construct_prompt(self, query: str, response: str) -> str:
        return f"""
        你是一个专业的用户内容偏好分析师，擅长从对话中提取用户的内容偏好信息。
        请从以下聊天记录中提取以下信息，如果某个信息不存在，请返回None。
        格式为JSON对象：
        {{
            "preferred_genres": "偏好内容类型(如: 动作,喜剧,科幻,悬疑)",
            "favorite_artists_or_celebrities": "喜欢的艺人或名人",
            "followed_entertainment_accounts": "关注的娱乐账号数量"
        }}

        聊天记录：
        用户询问: {query}
        回复内容: {response}
        """

    def _call_deepseek_api(self, prompt: str, retry: int = 0) -> Dict[str, Any]:
        if retry >= API_RETRY_TIMES:
            return {"error": f"Max retries ({API_RETRY_TIMES}) exceeded"}
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
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            logger.warning(f"API调用失败(尝试 {retry + 1}/{API_RETRY_TIMES}): {e}")
            time.sleep(2 ** retry)
            return self._call_deepseek_api(prompt, retry + 1)

    def _parse_api_response(self, api_response: Dict[str, Any]) -> Dict[str, Any]:
        default_response = {
            "preferred_genres": None,
            "favorite_artists_or_celebrities": None,
            "followed_entertainment_accounts": None
        }
        try:
            if "error" in api_response:
                logger.warning(f"API返回错误: {api_response['error']}")
                return default_response
            content = api_response["choices"][0]["message"]["content"]
            json_match = re.search(r'```jsons*(.*?)s*```', content, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
                parsed_data = json.loads(json_str)
            else:
                logger.warning("无法从响应中提取JSON内容")
                return default_response
            result = default_response.copy()
            for category in parsed_data:
                if category in result:
                    if isinstance(result[category], dict) and isinstance(parsed_data.get(category), dict):
                        for key in parsed_data[category]:
                            if key in result[category]:
                                result[category][key] = parsed_data[category].get(key)
                    else:
                        result[category] = parsed_data.get(category)
            return result
        except (KeyError, json.JSONDecodeError, IndexError, AttributeError, re.error) as e:
            logger.error(f"解析响应失败: {e}")
            return default_response

    def analyze_interaction(self, query: str, response: str) -> Dict[str, Any]:
        prompt = self._construct_prompt(query, response)
        api_response = self._call_deepseek_api(prompt)
        return self._parse_api_response(api_response)


def merge_values(current: Optional[str], new: Optional[str], separator: str = ', ') -> Optional[str]:
    if current == "None":
        current = None
    if new == "None":
        new = None
    if not current and not new:
        return None
    if not current:
        return new
    if not new:
        return current
    return f"{current}{separator}{new}"


def get_user_basic_info(user_id: str) -> Optional[Dict[str, Any]]:
    try:
        with db_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("""
                               SELECT DISTINCT user_id, user_name
                               FROM ods_user_interaction_di
                               WHERE user_id = %s
                               """, (user_id,))
                return cursor.fetchone()
    except DatabaseConnectionError as e:
        logger.error(f"数据库连接错误: {e}")
        return None
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
                return cursor.fetchall()
    except DatabaseConnectionError as e:
        logger.error(f"数据库连接错误: {e}")
        return []
    except Exception as e:
        logger.error(f"获取用户 {user_id} 交互记录时出错: {e}")
        return []


def initialize_user_profile(user_id: str, user_name: str) -> Dict[str, Any]:
    return {
        'user_id': user_id,
        'stat_date': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
        'user_name': user_name,
        'preferred_genres': None,
        'favorite_artists_or_celebrities': None,
        'followed_entertainment_accounts': None
    }


def analyze_user_interactions(user_id: str, extractor: UserProfileExtractor) -> Optional[Dict[str, Any]]:
    try:
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
            profile['preferred_genres'] = merge_values(profile['preferred_genres'], analysis['preferred_genres'])
            profile['favorite_artists_or_celebrities'] = merge_values(profile['favorite_artists_or_celebrities'], analysis['favorite_artists_or_celebrities'])
            profile['followed_entertainment_accounts'] = merge_values(profile['followed_entertainment_accounts'], analysis['followed_entertainment_accounts'])
        return profile
    except Exception as e:
        logger.error(f"处理用户 {user_id} 时发生未知错误: {e}")
        return None


def convert_value_to_db(value: Any) -> Optional[Any]:
    if isinstance(value, str):
        value = value.strip()
        if value == "None" or value == '':
            return None
    return value


def save_user_profile(profile: Dict[str, Any]) -> bool:
    """保存或更新单个用户画像到数据库，使用INSERT ... ON DUPLICATE KEY UPDATE优化写入和更新逻辑"""
    fields = [
        'user_id', 'stat_date', 'user_name', 'preferred_genres', 'favorite_artists_or_celebrities', 'followed_entertainment_accounts'
    ]
    try:
        with db_connection() as conn:
            with conn.cursor() as cursor:
                converted_profile = {k: convert_value_to_db(profile.get(k)) for k in fields}
                insert_sql = f"""
                    INSERT INTO dwd_user_content_preference_di (
                        {', '.join(fields)}, created_time, updated_time
                    ) VALUES (
                        {', '.join(['%s'] * len(fields))}, NOW(), NOW()
                    )
                    ON DUPLICATE KEY UPDATE
                        user_name = VALUES(user_name),
                        preferred_genres = VALUES(preferred_genres),
                        favorite_artists_or_celebrities = VALUES(favorite_artists_or_celebrities),
                        followed_entertainment_accounts = VALUES(followed_entertainment_accounts),
                        updated_time = NOW()
                """
                params = tuple(converted_profile[k] for k in fields)
                cursor.execute(insert_sql, params)
                conn.commit()
                return True
    except DatabaseConnectionError as e:
        logger.error(f"数据库连接错误: {e}")
        return False
    except Exception as e:
        logger.error(f"保存用户 {profile.get('user_id', '未知')} 画像时出错: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
        return False


def get_all_user_ids() -> List[str]:
    try:
        with db_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("""
                               SELECT DISTINCT user_id
                               FROM ods_user_interaction_di
                               ORDER BY user_id DESC
                               """)
                return [row['user_id'] for row in cursor.fetchall()]
    except DatabaseConnectionError as e:
        logger.error(f"数据库连接错误: {e}")
        return []
    except Exception as e:
        logger.error(f"获取用户ID列表时出错: {e}")
        return []


def process_all_users() -> None:
    start_time = time.time()
    logger.info("开始处理用户画像...")
    extractor = UserProfileExtractor()
    user_ids = get_all_user_ids()
    if not user_ids:
        logger.info("没有找到需要处理的用户")
        return
    logger.info(f"找到 {len(user_ids)} 个需要处理的用户")
    profiles = []
    success_count = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_userid = {executor.submit(analyze_user_interactions, user_id, extractor): user_id for user_id in user_ids}
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
                batch_success = 0
                for p in batch:
                    if save_user_profile(p):
                        batch_success += 1
                success_count += batch_success
                logger.info(f"已保存 {batch_success}/{len(batch)} 个用户画像 (累计: {success_count})")
                profiles = profiles[BATCH_SIZE:]
        # 保存剩余未满BATCH_SIZE的
        if profiles:
            batch_success = 0
            for p in profiles:
                if save_user_profile(p):
                    batch_success += 1
            success_count += batch_success
            logger.info(f"已保存 {batch_success}/{len(profiles)} 个用户画像 (累计: {success_count})")
    logger.info(f"用户画像数据生成完成，总耗时: {time.time() - start_time:.2f}秒")


if __name__ == "__main__":
    process_all_users() 