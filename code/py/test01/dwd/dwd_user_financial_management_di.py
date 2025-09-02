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
        你是一个专业的财务理财分析师，擅长从对话中提取用户的财务理财相关信息。
        请从以下聊天记录中提取以下财务理财相关信息，如果某个信息不存在，请返回None。
        格式为JSON对象：
        {{
            "user_id": "用户id",
            "stat_date": "统计日期，格式为YYYY-MM-DD",
            "user_name": "用户姓名",
            "total_assets": "总资产(元)",
            "total_liabilities": "总负债(元)",
            "net_worth": "净资产(元)",
            "monthly_income": "月收入(元)",
            "monthly_expenses": "月支出(元)",
            "savings_rate": "储蓄率(%)",
            "total_investments": "总投资金额(元)",
            "investment_return_rate": "投资回报率(%)",
            "risk_tolerance": "风险承受能力(如: 低 中 高)",
            "investment_experience": "投资经验年限(年)",
            "investment_frequency": "投资频率(如: 每日 每周 每月)",
            "cash_ratio": "现金类资产占比(%)",
            "fixed_income_ratio": "固定收益类资产占比(%)",
            "equity_ratio": "权益类资产占比(%)",
            "real_estate_ratio": "房地产类资产占比(%)",
            "alternative_investments_ratio": "另类投资类资产占比(%)",
            "financial_goal": "理财目标(如: 购房 子女教育 退休)",
            "goal_completion_percentage": "理财目标完成率(%)",
            "goal_time_horizon": "理财目标时间跨度(年)",
            "credit_score": "信用评分",
            "debt_to_income_ratio": "债务收入比(%)",
            "mortgage_status": "房贷状态(如: 无 有且还款中 已还清)",
            "car_loan_status": "车贷状态(如: 无 有且还款中 已还清)"
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
            "user_id": None,
            "stat_date": None,
            "user_name": None,
            "total_assets": None,
            "total_liabilities": None,
            "net_worth": None,
            "monthly_income": None,
            "monthly_expenses": None,
            "savings_rate": None,
            "total_investments": None,
            "investment_return_rate": None,
            "risk_tolerance": None,
            "investment_experience": None,
            "investment_frequency": None,
            "cash_ratio": None,
            "fixed_income_ratio": None,
            "equity_ratio": None,
            "real_estate_ratio": None,
            "alternative_investments_ratio": None,
            "financial_goal": None,
            "goal_completion_percentage": None,
            "goal_time_horizon": None,
            "credit_score": None,
            "debt_to_income_ratio": None,
            "mortgage_status": None,
            "car_loan_status": None
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
        'total_assets': None,
        'total_liabilities': None,
        'net_worth': None,
        'monthly_income': None,
        'monthly_expenses': None,
        'savings_rate': None,
        'total_investments': None,
        'investment_return_rate': None,
        'risk_tolerance': None,
        'investment_experience': None,
        'investment_frequency': None,
        'cash_ratio': None,
        'fixed_income_ratio': None,
        'equity_ratio': None,
        'real_estate_ratio': None,
        'alternative_investments_ratio': None,
        'financial_goal': None,
        'goal_completion_percentage': None,
        'goal_time_horizon': None,
        'credit_score': None,
        'debt_to_income_ratio': None,
        'mortgage_status': None,
        'car_loan_status': None
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
            profile['total_assets'] = merge_values(profile['total_assets'], analysis['total_assets'])
            profile['total_liabilities'] = merge_values(profile['total_liabilities'], analysis['total_liabilities'])
            profile['net_worth'] = merge_values(profile['net_worth'], analysis['net_worth'])
            profile['monthly_income'] = merge_values(profile['monthly_income'], analysis['monthly_income'])
            profile['monthly_expenses'] = merge_values(profile['monthly_expenses'], analysis['monthly_expenses'])
            profile['savings_rate'] = merge_values(profile['savings_rate'], analysis['savings_rate'])
            profile['total_investments'] = merge_values(profile['total_investments'], analysis['total_investments'])
            profile['investment_return_rate'] = merge_values(profile['investment_return_rate'], analysis['investment_return_rate'])
            profile['risk_tolerance'] = merge_values(profile['risk_tolerance'], analysis['risk_tolerance'])
            profile['investment_experience'] = merge_values(profile['investment_experience'], analysis['investment_experience'])
            profile['investment_frequency'] = merge_values(profile['investment_frequency'], analysis['investment_frequency'])
            profile['cash_ratio'] = merge_values(profile['cash_ratio'], analysis['cash_ratio'])
            profile['fixed_income_ratio'] = merge_values(profile['fixed_income_ratio'], analysis['fixed_income_ratio'])
            profile['equity_ratio'] = merge_values(profile['equity_ratio'], analysis['equity_ratio'])
            profile['real_estate_ratio'] = merge_values(profile['real_estate_ratio'], analysis['real_estate_ratio'])
            profile['alternative_investments_ratio'] = merge_values(profile['alternative_investments_ratio'], analysis['alternative_investments_ratio'])
            profile['financial_goal'] = merge_values(profile['financial_goal'], analysis['financial_goal'])
            profile['goal_completion_percentage'] = merge_values(profile['goal_completion_percentage'], analysis['goal_completion_percentage'])
            profile['goal_time_horizon'] = merge_values(profile['goal_time_horizon'], analysis['goal_time_horizon'])
            profile['credit_score'] = merge_values(profile['credit_score'], analysis['credit_score'])
            profile['debt_to_income_ratio'] = merge_values(profile['debt_to_income_ratio'], analysis['debt_to_income_ratio'])
            profile['mortgage_status'] = merge_values(profile['mortgage_status'], analysis['mortgage_status'])
            profile['car_loan_status'] = merge_values(profile['car_loan_status'], analysis['car_loan_status'])
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
        'user_id', 'stat_date', 'user_name', 'total_assets', 'total_liabilities', 'net_worth', 'monthly_income',
        'monthly_expenses', 'savings_rate', 'total_investments', 'investment_return_rate', 'risk_tolerance',
        'investment_experience', 'investment_frequency', 'cash_ratio', 'fixed_income_ratio', 'equity_ratio',
        'real_estate_ratio', 'alternative_investments_ratio', 'financial_goal', 'goal_completion_percentage',
        'goal_time_horizon', 'credit_score', 'debt_to_income_ratio', 'mortgage_status', 'car_loan_status'
    ]
    try:
        with db_connection() as conn:
            with conn.cursor() as cursor:
                converted_profile = {k: convert_value_to_db(profile.get(k)) for k in fields}
                insert_sql = f"""
                    INSERT INTO dwd_user_financial_management_di (
                        {', '.join(fields)}, created_time, updated_time
                    ) VALUES (
                        {', '.join(['%s'] * len(fields))}, NOW(), NOW()
                    )
                    ON DUPLICATE KEY UPDATE
                        user_name = VALUES(user_name),
                        total_assets = VALUES(total_assets),
                        total_liabilities = VALUES(total_liabilities),
                        net_worth = VALUES(net_worth),
                        monthly_income = VALUES(monthly_income),
                        monthly_expenses = VALUES(monthly_expenses),
                        savings_rate = VALUES(savings_rate),
                        total_investments = VALUES(total_investments),
                        investment_return_rate = VALUES(investment_return_rate),
                        risk_tolerance = VALUES(risk_tolerance),
                        investment_experience = VALUES(investment_experience),
                        investment_frequency = VALUES(investment_frequency),
                        cash_ratio = VALUES(cash_ratio),
                        fixed_income_ratio = VALUES(fixed_income_ratio),
                        equity_ratio = VALUES(equity_ratio),
                        real_estate_ratio = VALUES(real_estate_ratio),
                        alternative_investments_ratio = VALUES(alternative_investments_ratio),
                        financial_goal = VALUES(financial_goal),
                        goal_completion_percentage = VALUES(goal_completion_percentage),
                        goal_time_horizon = VALUES(goal_time_horizon),
                        credit_score = VALUES(credit_score),
                        debt_to_income_ratio = VALUES(debt_to_income_ratio),
                        mortgage_status = VALUES(mortgage_status),
                        car_loan_status = VALUES(car_loan_status),
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
