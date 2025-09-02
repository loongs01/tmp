import os
import re
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import mysql.connector
from contextlib import contextmanager
from transformers import pipeline
from functools import lru_cache

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

# 设置本地缓存目录
os.environ["TRANSFORMERS_CACHE"] = "D:/transformers_cache"
os.environ["TRANSFORMERS_OFFLINE"] = "1"  # 强制离线模式

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
MAX_WORKERS = 5
BATCH_SIZE = 10
MODEL_LOAD_RETRIES = 3


class DatabaseConnectionError(Exception):
    """自定义数据库连接错误"""
    pass


@contextmanager
def db_connection():
    """数据库连接上下文管理器"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
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


@lru_cache(maxsize=1)
def load_nlp_model():
    """加载本地NLP模型，使用缓存避免重复加载"""
    for attempt in range(MODEL_LOAD_RETRIES):
        try:
            model = pipeline(
                "text-classification",
                model="distilbert-base-uncased-finetuned-sst-2-english",
                device=-1  # 强制使用CPU
            )
            logger.info("NLP模型加载成功")
            return model
        except Exception as e:
            logger.warning(f"模型加载尝试 {attempt + 1}/{MODEL_LOAD_RETRIES} 失败: {str(e)}")
            if attempt == MODEL_LOAD_RETRIES - 1:
                logger.warning("无法加载NLP模型，将使用简化分析模式")
                return None
            time.sleep(2 ** attempt)  # 指数退避


class OfflineProfileExtractor:
    """完全离线的用户画像提取器"""

    def __init__(self):
        self.nlp_model = load_nlp_model()
        # 关键词配置
        self.food_keywords = ["喜欢", "爱吃", "偏好", "最爱", "常吃"]
        self.restriction_keywords = ["不吃", "忌口", "过敏", "不能吃", "避免", "素食", "麸质"]
        self.color_keywords = ["颜色", "色彩", "色系", "色调"]
        self.hobby_keywords = ["爱好", "兴趣", "喜欢做", "常做", "擅长"]
        self.game_keywords = ["游戏", "手游", "网游", "电竞", "王者", "吃鸡"]
        self.book_keywords = ["书", "小说", "读物", "名著", "阅读", "文学"]
        self.movie_keywords = ["电影", "影片", "大片", "影剧", "院线"]
        self.achievement_keywords = ["成就", "获奖", "荣誉", "成绩"]
        self.dream_keywords = ["梦想", "理想", "愿望", "想成为"]
        self.plan_keywords = ["计划", "打算", "安排", "目标"]
        self.thought_keywords = ["想法", "认为", "觉得", "观点"]
        self.inspiration_keywords = ["灵感", "启发", "感悟", "体会"]
        self.family_keywords = ["家人", "父母", "孩子", "家庭"]
        self.friend_keywords = ["朋友", "同学", "闺蜜", "兄弟"]
        self.colleague_keywords = ["同事", "上司", "老板", "团队"]
        self.travel_keywords = ["旅行", "旅游", "去过", "游玩"]
        self.desired_travel_keywords = ["想去", "计划去", "梦想去", "希望去"]

    def extract_by_keywords(self, text: str, keywords: List[str]) -> List[str]:
        """基于关键词提取信息"""
        results = set()
        for keyword in keywords:
            pattern = re.compile(f"{keyword}[：:]*([^。，；;\n]+)")
            matches = pattern.findall(text)
            for match in matches:
                cleaned = re.sub(r"[^\w\s]", "", match).strip()
                if cleaned:
                    results.add(cleaned)
        return list(results) if results else None

    def extract_text_sentiment(self, text: str) -> Optional[str]:
        """分析文本情感倾向"""
        if not self.nlp_model:
            return None
        try:
            result = self.nlp_model(text[:512])  # 截断长文本
            return result[0]['label']
        except Exception as e:
            logger.warning(f"情感分析失败: {str(e)}")
            return None

    def analyze_interaction(self, query: str, response: str) -> Dict[str, Any]:
        """分析单条交互记录"""
        combined_text = f"{query} {response}"

        return {
            # 饮食偏好
            "favorite_foods": self.extract_by_keywords(combined_text, self.food_keywords),
            "dietary_restrictions": self.extract_by_keywords(combined_text, self.restriction_keywords),

            # 颜色偏好
            "favorite_colors": self.extract_colors(combined_text),

            # 兴趣爱好
            "hobbies": self.extract_by_keywords(combined_text, self.hobby_keywords),
            "favorite_games": self.extract_by_keywords(combined_text, self.game_keywords),
            "favorite_books": self.extract_by_keywords(combined_text, self.book_keywords),
            "favorite_movies": self.extract_by_keywords(combined_text, self.movie_keywords),

            # 个人发展
            "achievements": self.extract_by_keywords(combined_text, self.achievement_keywords),
            "dreams": self.extract_by_keywords(combined_text, self.dream_keywords),
            "ideals": self.extract_by_keywords(combined_text, self.dream_keywords),  # 理想与梦想使用相同关键词
            "plans": self.extract_by_keywords(combined_text, self.plan_keywords),
            "goals": self.extract_by_keywords(combined_text, self.plan_keywords),  # 目标与计划使用相同关键词

            # 想法与灵感
            "thoughts": self.extract_thoughts(combined_text),
            "inspirations": self.extract_by_keywords(combined_text, self.inspiration_keywords),

            # 记忆
            "family_memories": self.extract_memories(combined_text, self.family_keywords),
            "friend_memories": self.extract_memories(combined_text, self.friend_keywords),
            "colleague_memories": self.extract_memories(combined_text, self.colleague_keywords),

            # 旅行信息
            "visited_places": self.extract_by_keywords(combined_text, self.travel_keywords),
            "desired_travel_places": self.extract_by_keywords(combined_text, self.desired_travel_keywords),

            # 情感分析
            "sentiment": self.extract_text_sentiment(combined_text)
        }

    def extract_colors(self, text: str) -> Optional[List[str]]:
        """提取颜色信息"""
        if any(kw in text for kw in self.color_keywords):
            colors = re.findall(r"(红色|蓝色|绿色|黄色|黑色|白色|紫色|粉色|橙色|金色|银色)", text)
            return list(set(colors)) if colors else None
        return None

    def extract_thoughts(self, text: str) -> Optional[str]:
        """提取想法（更长的文本）"""
        thoughts = []
        for keyword in self.thought_keywords:
            pattern = re.compile(f"{keyword}[：:]*([^。，；;\n]+)")
            matches = pattern.findall(text)
            thoughts.extend(matches)
        return "。".join(thoughts) if thoughts else None

    def extract_memories(self, text: str, keywords: List[str]) -> Optional[str]:
        """提取记忆（更长的文本）"""
        memories = []
        for keyword in keywords:
            pattern = re.compile(f"{keyword}[：:]*([^。，；;\n]+)")
            matches = pattern.findall(text)
            memories.extend(matches)
        return "。".join(memories) if memories else None


def merge_values(current: Optional[Any], new: Optional[Any]) -> Optional[Any]:
    """合并两个值，处理各种类型"""
    if not current:
        return new
    if not new:
        return current

    # 处理文本类型（thoughts/memories等）
    if isinstance(current, str) and isinstance(new, str):
        if current.endswith(('。', '!', '?')):
            return f"{current} {new}"
        return f"{current}。{new}"

    # 处理列表类型
    if isinstance(current, list) and isinstance(new, list):
        return list(set(current + new))
    elif isinstance(current, list):
        return current + [new] if new not in current else current
    elif isinstance(new, list):
        return [current] + new if current not in new else new

    # 默认字符串连接
    if isinstance(current, str) and isinstance(new, str):
        if current == new:
            return current
        return f"{current}, {new}"

    return new


def get_user_ids() -> List[str]:
    """获取所有需要处理的用户ID"""
    try:
        with db_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT DISTINCT user_id FROM ods_user_interaction_di ORDER BY user_id DESC")
                return [row['user_id'] for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"获取用户ID列表失败: {str(e)}")
        return []


def get_user_interactions(user_id: str) -> List[Dict[str, Any]]:
    """获取用户的所有交互记录"""
    try:
        with db_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("""
                               SELECT query_text, response_text
                               FROM ods_user_interaction_di
                               WHERE user_id = %s
                               ORDER BY interaction_time
                               """, (user_id,))
                return cursor.fetchall()
    except Exception as e:
        logger.error(f"获取用户 {user_id} 交互记录失败: {str(e)}")
        return []


def analyze_user(user_id: str, extractor: OfflineProfileExtractor) -> Optional[Dict[str, Any]]:
    """分析单个用户"""
    try:
        # 获取用户基本信息
        with db_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("""
                               SELECT DISTINCT user_id, user_name
                               FROM ods_user_interaction_di
                               WHERE user_id = %s
                               """, (user_id,))
                user_info = cursor.fetchone()
                if not user_info:
                    logger.warning(f"未找到用户 {user_id} 的基本信息")
                    return None

        # 初始化用户画像 - 与dwd_user_comprehensive_profile_di_0627表结构完全匹配
        profile = {
            'user_id': user_id,
            'stat_date': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
            'user_name': user_info['user_name'],

            # 饮食偏好
            'favorite_foods': None,
            'dietary_restrictions': None,

            # 颜色偏好
            'favorite_colors': None,

            # 兴趣爱好
            'hobbies': None,
            'favorite_games': None,
            'favorite_books': None,
            'favorite_movies': None,

            # 个人发展
            'achievements': None,
            'dreams': None,
            'ideals': None,
            'plans': None,
            'goals': None,

            # 想法与灵感
            'thoughts': None,
            'inspirations': None,

            # 记忆
            'family_memories': None,
            'friend_memories': None,
            'colleague_memories': None,

            # 旅行信息
            'visited_places': None,
            'desired_travel_places': None
        }

        # 获取并分析交互记录
        interactions = get_user_interactions(user_id)
        if not interactions:
            logger.info(f"用户 {user_id} 没有交互记录")
            return profile

        for interaction in interactions:
            analysis = extractor.analyze_interaction(
                interaction['query_text'],
                interaction['response_text']
            )

            # 合并分析结果
            for field in profile.keys():
                if field in analysis and analysis[field]:
                    profile[field] = merge_values(profile[field], analysis[field])

        return profile
    except Exception as e:
        logger.error(f"处理用户 {user_id} 时发生错误: {str(e)}")
        return None


def save_profile(profile: Dict[str, Any]) -> bool:
    """保存用户画像到数据库"""
    try:
        with db_connection() as conn:
            with conn.cursor() as cursor:
                # 检查记录是否存在
                cursor.execute("""
                               SELECT 1
                               FROM dwd_user_comprehensive_profile_di_0627
                               WHERE user_id = %s
                                 AND stat_date = %s
                               """, (profile['user_id'], profile['stat_date']))

                # 准备数据 - 确保所有字段都被正确处理
                def prepare_value(value):
                    """准备数据库字段值，确保列表等类型被正确转换为字符串"""
                    if value is None:
                        return None
                    if isinstance(value, list):
                        # 将列表转换为逗号分隔的字符串
                        return ", ".join(str(item) for item in value)
                    if isinstance(value, dict):
                        # 字典类型转换为JSON字符串
                        return json.dumps(value, ensure_ascii=False)
                    return str(value) if not isinstance(value, (str, int, float)) else value

                data = {
                    'user_name': profile['user_name'],

                    # 饮食偏好
                    'favorite_foods': prepare_value(profile['favorite_foods']),
                    'dietary_restrictions': prepare_value(profile['dietary_restrictions']),

                    # 颜色偏好
                    'favorite_colors': prepare_value(profile['favorite_colors']),

                    # 兴趣爱好
                    'hobbies': prepare_value(profile['hobbies']),
                    'favorite_games': prepare_value(profile['favorite_games']),
                    'favorite_books': prepare_value(profile['favorite_books']),
                    'favorite_movies': prepare_value(profile['favorite_movies']),

                    # 个人发展
                    'achievements': prepare_value(profile['achievements']),
                    'dreams': prepare_value(profile['dreams']),
                    'ideals': prepare_value(profile['ideals']),
                    'plans': prepare_value(profile['plans']),
                    'goals': prepare_value(profile['goals']),

                    # 想法与灵感
                    'thoughts': prepare_value(profile['thoughts']),
                    'inspirations': prepare_value(profile['inspirations']),

                    # 记忆
                    'family_memories': prepare_value(profile['family_memories']),
                    'friend_memories': prepare_value(profile['friend_memories']),
                    'colleague_memories': prepare_value(profile['colleague_memories']),

                    # 旅行信息
                    'visited_places': prepare_value(profile['visited_places']),
                    'desired_travel_places': prepare_value(profile['desired_travel_places'])
                }

                if cursor.fetchone():
                    # 更新现有记录
                    cursor.execute("""
                                   UPDATE dwd_user_comprehensive_profile_di_0627
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
                                   """, (
                                       data['user_name'],
                                       data['favorite_foods'],
                                       data['dietary_restrictions'],
                                       data['favorite_colors'],
                                       data['hobbies'],
                                       data['favorite_games'],
                                       data['favorite_books'],
                                       data['favorite_movies'],
                                       data['achievements'],
                                       data['dreams'],
                                       data['ideals'],
                                       data['plans'],
                                       data['goals'],
                                       data['thoughts'],
                                       data['inspirations'],
                                       data['family_memories'],
                                       data['friend_memories'],
                                       data['colleague_memories'],
                                       data['visited_places'],
                                       data['desired_travel_places'],
                                       profile['user_id'],
                                       profile['stat_date']
                                   ))
                else:
                    # 插入新记录
                    cursor.execute("""
                                   INSERT INTO dwd_user_comprehensive_profile_di_0627 (user_id, stat_date, user_name,
                                                                                       favorite_foods,
                                                                                       dietary_restrictions,
                                                                                       favorite_colors, hobbies,
                                                                                       favorite_games,
                                                                                       favorite_books, favorite_movies,
                                                                                       achievements, dreams, ideals,
                                                                                       plans, goals, thoughts,
                                                                                       inspirations, family_memories,
                                                                                       friend_memories,
                                                                                       colleague_memories,
                                                                                       visited_places,
                                                                                       desired_travel_places,
                                                                                       created_time, updated_time)
                                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                           %s, %s, NOW(), NOW())
                                   """, (
                                       profile['user_id'],
                                       profile['stat_date'],
                                       data['user_name'],
                                       data['favorite_foods'],
                                       data['dietary_restrictions'],
                                       data['favorite_colors'],
                                       data['hobbies'],
                                       data['favorite_games'],
                                       data['favorite_books'],
                                       data['favorite_movies'],
                                       data['achievements'],
                                       data['dreams'],
                                       data['ideals'],
                                       data['plans'],
                                       data['goals'],
                                       data['thoughts'],
                                       data['inspirations'],
                                       data['family_memories'],
                                       data['friend_memories'],
                                       data['colleague_memories'],
                                       data['visited_places'],
                                       data['desired_travel_places']
                                   ))

                conn.commit()
                return True
    except mysql.connector.Error as e:
        logger.error(f"数据库错误: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"保存用户 {profile['user_id']} 画像失败: {str(e)}")
        return False


def process_batch(user_ids: List[str], extractor: OfflineProfileExtractor) -> int:
    """处理一批用户"""
    success_count = 0
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(user_ids))) as executor:
        futures = {executor.submit(analyze_user, user_id, extractor): user_id for user_id in user_ids}

        for future in as_completed(futures):
            user_id = futures[future]
            try:
                profile = future.result()
                if profile and save_profile(profile):
                    success_count += 1
                    logger.info(f"成功处理用户 {user_id}")
                elif profile:
                    logger.warning(f"保存用户 {user_id} 画像失败")
                else:
                    logger.warning(f"分析用户 {user_id} 失败")
            except Exception as e:
                logger.error(f"处理用户 {user_id} 时出错: {str(e)}")

    return success_count


def main():
    """主处理函数"""
    logger.info("开始离线用户画像分析...")
    start_time = time.time()

    try:
        extractor = OfflineProfileExtractor()
    except Exception as e:
        logger.error(f"初始化分析器失败: {str(e)}")
        return

    user_ids = get_user_ids()
    if not user_ids:
        logger.info("没有需要处理的用户")
        return

    logger.info(f"共找到 {len(user_ids)} 个需要处理的用户")

    total_success = 0
    for i in range(0, len(user_ids), BATCH_SIZE):
        batch_ids = user_ids[i:i + BATCH_SIZE]
        logger.info(
            f"正在处理批次 {i // BATCH_SIZE + 1}/{(len(user_ids) - 1) // BATCH_SIZE + 1} (用户数: {len(batch_ids)})")

        success = process_batch(batch_ids, extractor)
        total_success += success
        logger.info(f"本批次成功处理 {success}/{len(batch_ids)} 个用户")

    total_time = time.time() - start_time
    logger.info(
        f"处理完成，总耗时: {total_time:.2f}秒\n"
        f"成功处理用户数: {total_success}/{len(user_ids)}\n"
        f"平均速度: {total_success / max(total_time, 1):.2f} 用户/秒"
    )


if __name__ == "__main__":
    main()
