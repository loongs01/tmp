import os
import json
import re
import configparser
import mysql.connector
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from logging import getLogger, INFO, basicConfig
from transformers import pipeline
from contextlib import contextmanager

# 配置日志
basicConfig(level=INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = getLogger(__name__)

# 初始化配置
try:
    sentiment_pipeline = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")
except Exception as e:
    logger.warning(f"情感分析管道初始化失败: {str(e)}")
    sentiment_pipeline = None

# 配置管理
config = configparser.ConfigParser()
config_file_path = os.path.join(os.path.dirname(os.getcwd()), 'init', 'config.ini')
config.read(config_file_path)

DEEPSEEK_API_KEY = config.get('deepseek', 'deepseek.api_key', fallback="")
DEEPSEEK_ENDPOINT = config.get('deepseek', 'deepseek.endpoint', fallback="https://api.deepseek.com/v1/chat/completions")
DEEPSEEK_MODEL = config.get('deepseek', 'deepseek.model', fallback="deepseek-chat")

# MySQL数据库配置
db_config = {
    'host': '192.168.10.105',
    'user': 'licz.1',
    'password': 'GjFmT5NEiE',
    'database': 'sq_liufengdb',
    'charset': 'utf8mb4',
    'connect_timeout': 10
}

# 全局配置
MAX_WORKERS = 10  # 最大线程数
BATCH_SIZE = 5  # 批量处理大小
API_RETRY_TIMES = 3  # API调用重试次数


class DatabaseConnectionError(Exception):
    """自定义数据库连接错误"""
    pass


@contextmanager
def db_connection():
    """数据库连接上下文管理器"""
    try:
        conn = mysql.connector.connect(**db_config)
        logger.debug("数据库连接成功")
        yield conn
    except mysql.connector.Error as e:
        raise DatabaseConnectionError(f"数据库连接失败: {str(e)}")
    finally:
        if 'conn' in locals():
            try:
                conn.close()
                logger.debug("数据库连接已关闭")
            except Exception as e:
                logger.warning(f"关闭数据库连接时出错: {str(e)}")


class UserProfileExtractor:
    """用户画像提取器，使用DeepSeek API分析对话数据"""

    def __init__(self, api_key: str = None, endpoint: str = None, model: str = None):
        """初始化用户画像提取器"""
        self.api_key = api_key or DEEPSEEK_API_KEY
        self.endpoint = endpoint or DEEPSEEK_ENDPOINT
        self.model = model or DEEPSEEK_MODEL

        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        self.api_url = self.endpoint
        self.session = requests.Session()  # 使用会话保持连接

    def _construct_prompt(self, query: str, response: str) -> str:
        """构建全面的提示词"""
        return f"""
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

    def _call_deepseek_api(self, prompt: str, retry: int = 0) -> Dict[str, Any]:
        """调用DeepSeek API，带重试机制"""
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
                self.api_url,
                headers=self.headers,
                data=json.dumps(payload),
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            logger.warning(f"API调用失败(尝试 {retry + 1}/{API_RETRY_TIMES}): {e}")
            time.sleep(2 ** retry)  # 指数退避
            return self._call_deepseek_api(prompt, retry + 1)

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
                logger.warning(f"API返回错误: {api_response['error']}")
                return default_response

            content = api_response["choices"][0]["message"]["content"]

            # 提取JSON内容（去除```json和```标记）
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
        """分析单条交互记录"""
        prompt = self._construct_prompt(query, response)
        api_response = self._call_deepseek_api(prompt)
        return self._parse_api_response(api_response)


def merge_values(current: Optional[str], new: Optional[str], separator: str = ', ') -> Optional[str]:
    """合并两个值，处理None情况"""
    # 将字符串"None"转换为None
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
    """获取用户基本信息"""
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
    """获取用户的所有交互记录"""
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
    """初始化用户画像"""
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


def analyze_user_interactions(user_id: str, extractor: UserProfileExtractor) -> Optional[Dict[str, Any]]:
    """分析单个用户的交互数据并生成画像"""
    try:
        # 获取用户基本信息
        user_info = get_user_basic_info(user_id)
        if not user_info:
            logger.warning(f"未找到用户 {user_id} 的基本信息")
            return None

        # 初始化用户画像
        profile = initialize_user_profile(user_info['user_id'], user_info['user_name'])

        # 获取用户交互记录
        interactions = get_user_interactions(user_id)
        if not interactions:
            logger.info(f"用户 {user_id} 没有交互记录")
            return profile  # 返回空画像

        # 分析每条交互记录
        for interaction in interactions:
            query = interaction['query_text']
            response = interaction['response_text']

            # 使用DeepSeek分析交互记录
            analysis = extractor.analyze_interaction(query, response)

            # 合并分析结果到用户画像
            profile['favorite_foods'] = merge_values(profile['favorite_foods'],
                                                     analysis['饮食偏好']['favorite_foods'])
            profile['dietary_restrictions'] = merge_values(profile['dietary_restrictions'],
                                                           analysis['饮食偏好']['dietary_restrictions'])
            profile['favorite_colors'] = merge_values(profile['favorite_colors'],
                                                      analysis['颜色偏好']['favorite_colors'])
            profile['hobbies'] = merge_values(profile['hobbies'], analysis['兴趣爱好']['hobbies'])
            profile['favorite_games'] = merge_values(profile['favorite_games'],
                                                     analysis['兴趣爱好']['favorite_games'])
            profile['favorite_books'] = merge_values(profile['favorite_books'],
                                                     analysis['兴趣爱好']['favorite_books'])
            profile['favorite_movies'] = merge_values(profile['favorite_movies'],
                                                      analysis['兴趣爱好']['favorite_movies'])
            profile['achievements'] = merge_values(profile['achievements'], analysis['个人发展']['achievements'])
            profile['dreams'] = merge_values(profile['dreams'], analysis['个人发展']['dreams'])
            profile['ideals'] = merge_values(profile['ideals'], analysis['个人发展']['ideals'])
            profile['plans'] = merge_values(profile['plans'], analysis['个人发展']['plans'])
            profile['goals'] = merge_values(profile['goals'], analysis['个人发展']['goals'])
            profile['thoughts'] = merge_values(profile['thoughts'], analysis['想法与灵感']['thoughts'])
            profile['inspirations'] = merge_values(profile['inspirations'], analysis['想法与灵感']['inspirations'])
            profile['family_memories'] = merge_values(profile['family_memories'],
                                                      analysis['记忆']['family_memories'])
            profile['friend_memories'] = merge_values(profile['friend_memories'],
                                                      analysis['记忆']['friend_memories'])
            profile['colleague_memories'] = merge_values(profile['colleague_memories'],
                                                         analysis['记忆']['colleague_memories'])

            # 特殊处理地点信息
            if analysis['旅行信息']['visited_places']:
                profile['visited_places'] = merge_values(profile['visited_places'],
                                                         analysis['旅行信息']['visited_places'])
            if analysis['旅行信息']['desired_travel_places']:
                profile['desired_travel_places'] = merge_values(profile['desired_travel_places'],
                                                                analysis['旅行信息']['desired_travel_places'])

        return profile
    except Exception as e:
        logger.error(f"处理用户 {user_id} 时发生未知错误: {e}")
        return None


def convert_value_to_db(value: Any) -> Optional[Any]:
    """将值转换为适合数据库存储的格式"""
    if isinstance(value, str):
        value = value.strip()
        if value == "None" or value == '':
            return None
    return value


def save_user_profile(profile: Dict[str, Any]) -> bool:
    """保存单个用户画像到数据库"""
    try:
        with db_connection() as conn:
            with conn.cursor() as cursor:
                # 检查是否存在
                cursor.execute("""
                               SELECT 1
                               FROM dwd_user_comprehensive_profile_di_0625
                               WHERE user_id = %s
                                 AND stat_date = %s
                               """, (profile['user_id'], profile['stat_date']))

                # 转换值为数据库友好格式
                converted_profile = {k: convert_value_to_db(v) for k, v in profile.items()}

                if cursor.fetchone():
                    # 更新现有记录
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
                    params = (
                        converted_profile['user_name'],
                        converted_profile['favorite_foods'],
                        converted_profile['dietary_restrictions'],
                        converted_profile['favorite_colors'],
                        converted_profile['hobbies'],
                        converted_profile['favorite_games'],
                        converted_profile['favorite_books'],
                        converted_profile['favorite_movies'],
                        converted_profile['achievements'],
                        converted_profile['dreams'],
                        converted_profile['ideals'],
                        converted_profile['plans'],
                        converted_profile['goals'],
                        converted_profile['thoughts'],
                        converted_profile['inspirations'],
                        converted_profile['family_memories'],
                        converted_profile['friend_memories'],
                        converted_profile['colleague_memories'],
                        converted_profile['visited_places'],
                        converted_profile['desired_travel_places'],
                        converted_profile['user_id'],
                        converted_profile['stat_date']
                    )
                    cursor.execute(update_sql, params)
                else:
                    # 插入新记录
                    insert_sql = """
                                 INSERT INTO dwd_user_comprehensive_profile_di_0625 (user_id, stat_date, user_name, 
                                                                                     favorite_foods, 
                                                                                     dietary_restrictions, 
                                                                                     favorite_colors, 
                                                                                     hobbies, favorite_games, 
                                                                                     favorite_books, favorite_movies, 
                                                                                     achievements, dreams, ideals, 
                                                                                     plans, goals, thoughts, 
                                                                                     inspirations, family_memories, 
                                                                                     friend_memories, 
                                                                                     colleague_memories, 
                                                                                     visited_places, 
                                                                                     desired_travel_places, 
                                                                                     created_time, updated_time)
                                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                         %s, %s, NOW(), NOW()) 
                                 """
                    params = (
                        converted_profile['user_id'],
                        converted_profile['stat_date'],
                        converted_profile['user_name'],
                        converted_profile['favorite_foods'],
                        converted_profile['dietary_restrictions'],
                        converted_profile['favorite_colors'],
                        converted_profile['hobbies'],
                        converted_profile['favorite_games'],
                        converted_profile['favorite_books'],
                        converted_profile['favorite_movies'],
                        converted_profile['achievements'],
                        converted_profile['dreams'],
                        converted_profile['ideals'],
                        converted_profile['plans'],
                        converted_profile['goals'],
                        converted_profile['thoughts'],
                        converted_profile['inspirations'],
                        converted_profile['family_memories'],
                        converted_profile['friend_memories'],
                        converted_profile['colleague_memories'],
                        converted_profile['visited_places'],
                        converted_profile['desired_travel_places']
                    )
                    cursor.execute(insert_sql, params)

                conn.commit()
                return True
    except DatabaseConnectionError as e:
        logger.error(f"数据库连接错误: {e}")
        return False
    except Exception as e:
        logger.error(f"保存用户 {profile['user_id']} 画像时出错: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False


def get_all_user_ids() -> List[str]:
    """获取所有有交互记录的用户ID"""
    try:
        with db_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("""
                               SELECT DISTINCT user_id
                               FROM ods_user_interaction_di
                               order by user_id desc
                               """)
                return [row['user_id'] for row in cursor.fetchall()]
    except DatabaseConnectionError as e:
        logger.error(f"数据库连接错误: {e}")
        return []
    except Exception as e:
        logger.error(f"获取用户ID列表时出错: {e}")
        return []


def process_user_batch(user_ids: List[str], extractor: UserProfileExtractor) -> List[Dict[str, Any]]:
    """处理一批用户并返回画像列表"""
    profiles = []
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(user_ids))) as executor:
        futures = {executor.submit(analyze_user_interactions, user_id, extractor): user_id for user_id in user_ids}

        for future in as_completed(futures):
            user_id = futures[future]
            try:
                profile = future.result()
                if profile:
                    profiles.append(profile)
                    logger.info(f"成功处理用户 {user_id}")
                else:
                    logger.info(f"用户 {user_id} 没有有效数据")
            except Exception as e:
                logger.error(f"处理用户 {user_id} 时发生错误: {e}")

    return profiles


def process_all_users() -> None:
    """处理所有有交互记录的用户"""
    start_time = time.time()
    logger.info("开始处理用户画像...")

    # 初始化DeepSeek提取器
    extractor = UserProfileExtractor()

    # 获取所有用户ID
    user_ids = get_all_user_ids()
    if not user_ids:
        logger.info("没有找到需要处理的用户")
        return

    logger.info(f"找到 {len(user_ids)} 个需要处理的用户")

    # 分批处理用户
    for i in range(0, len(user_ids), BATCH_SIZE):
        batch_user_ids = user_ids[i:i + BATCH_SIZE]
        logger.info(
            f"正在处理批次 {i // BATCH_SIZE + 1}/{len(user_ids) // BATCH_SIZE + 1}, 用户ID: {batch_user_ids[0:5]}...共{len(batch_user_ids)}个用户")

        # 处理当前批次
        profiles = process_user_batch(batch_user_ids, extractor)

        # 保存当前批次
        if profiles:
            success_count = 0
            for profile in profiles:
                if save_user_profile(profile):
                    success_count += 1
            logger.info(f"已保存 {success_count}/{len(profiles)} 个用户画像")
        else:
            logger.info("本批次没有需要保存的用户画像")

    logger.info(f"用户画像数据生成完成，总耗时: {time.time() - start_time:.2f}秒")


if __name__ == "__main__":
    process_all_users()
