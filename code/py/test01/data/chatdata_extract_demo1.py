import mysql.connector
import json
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Optional, Any
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UserProfileGenerator:
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.conn = None
        self.cursor = None

        # 初始化兴趣关键词和主题分类（可以从配置文件或数据库加载）
        self.interest_keywords = {
            '美食': ['食谱', '烹饪', '寿司', '牛排', '烘焙'],
            '游戏': ['游戏', '英雄联盟', 'CSGO', '版本更新', '攻略'],
            '阅读': ['书', '小说', '文学', '阅读', '作家'],
            '旅行': ['旅行', '旅游', '景点', '攻略', '签证'],
            '电影': ['电影', '影评', '导演', '演员', '奥斯卡'],
            '音乐': ['音乐', '作曲', '乐器', '乐理', '演唱会'],
            '运动': ['健身', '篮球', '训练', '运动', '比赛'],
            '科技': ['编程', 'AI', '科技', '算法', '计算机'],
            '艺术': ['绘画', '艺术', '设计', '创作', '展览'],
            '植物': ['植物', '园艺', '种植', '花卉', '养护']
        }

        self.response_topics = {
            '饮食': ['食谱', '食材', '烹饪', '饮食', '营养'],
            '娱乐': ['游戏', '电影', '音乐', '娱乐', '休闲'],
            '学习': ['学习', '教育', '阅读', '知识', '课程'],
            '健康': ['健康', '健身', '医疗', '养生', '锻炼'],
            '科技': ['技术', '科学', '创新', '数字', '智能']
        }

    def connect(self) -> None:
        """建立数据库连接"""
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            self.cursor = self.conn.cursor(dictionary=True)  # 使用字典游标
            logger.info("数据库连接成功")
        except Exception as e:
            logger.error(f"数据库连接失败: {str(e)}")
            raise

    def disconnect(self) -> None:
        """关闭数据库连接"""
        if self.conn:
            try:
                self.conn.close()
                logger.info("数据库连接已关闭")
            except Exception as e:
                logger.error(f"关闭数据库连接时出错: {str(e)}")

    def __enter__(self):
        """支持上下文管理协议"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """支持上下文管理协议"""
        self.disconnect()

    def analyze_interactions(self) -> List[Dict[str, Any]]:
        """分析交互数据并生成用户画像"""
        logger.info("开始分析用户交互数据...")

        try:
            # 1. 获取所有用户的交互数据
            query = """
                    SELECT user_id,
                           user_name,
                           interaction_type,
                           interaction_mode,
                           query_text,
                           response_text,
                           satisfaction_score,
                           model_name,
                           rag_config,
                           is_rag_used,
                           interaction_time
                    FROM ods_user_interaction_di
                    ORDER BY user_id, interaction_time
                    """
            self.cursor.execute(query)
            interactions = self.cursor.fetchall()
            print(interactions[0]['user_id'])
            for interaction in interactions:
                print(interaction)

            if not interactions:
                logger.warning("没有找到交互数据")
                return []

            # 2. 按用户分组分析
            user_profiles = defaultdict(dict)
            for interaction in interactions:
                self._process_interaction(interaction, user_profiles)

            # 3. 生成结构化画像数据
            structured_profiles = []
            for user_id, profile in user_profiles.items():
                structured_profile = self._structure_profile(profile)
                structured_profiles.append(structured_profile)

            return structured_profiles

        except Exception as e:
            logger.error(f"分析交互数据时出错: {str(e)}")
            self.conn.rollback()
            raise

    def _process_interaction(self, interaction: Dict[str, Any], user_profiles: defaultdict) -> None:
        """处理单个交互记录"""
        user_id = interaction['user_id']
        if user_id not in user_profiles:
            user_profiles[user_id] = {
                'user_id': user_id,
                'user_name': interaction['user_name'],
                'interaction_count': 0,
                'interests': defaultdict(int),
                'preferred_modes': defaultdict(int),
                'satisfaction_scores': [],
                'last_interaction_time': interaction['interaction_time'],
                'query_topics': defaultdict(int)
            }

        profile = user_profiles[user_id]
        profile['interaction_count'] += 1
        profile['last_interaction_time'] = max(
            profile['last_interaction_time'],
            interaction['interaction_time']
        )

        if interaction['satisfaction_score'] is not None:
            profile['satisfaction_scores'].append(interaction['satisfaction_score'])

        # 分析交互模式偏好
        profile['preferred_modes'][interaction['interaction_mode']] += 1

        # 从查询文本中提取兴趣关键词
        self._extract_interests_from_query(interaction['query_text'], profile)

        # 分析响应内容中的主题
        self._analyze_response_content(interaction['response_text'], profile)

    def _extract_interests_from_query(self, query_text: Optional[str], profile: Dict[str, Any]) -> None:
        """从查询文本中提取兴趣关键词"""
        if not query_text:
            return

        for category, keywords in self.interest_keywords.items():
            for keyword in keywords:
                if keyword.lower() in query_text.lower():
                    profile['interests'][category] += 1
                    break

    def _analyze_response_content(self, response_text: Optional[str], profile: Dict[str, Any]) -> None:
        """分析响应内容中的主题"""
        if not response_text:
            return

        for topic, keywords in self.response_topics.items():
            for keyword in keywords:
                if keyword.lower() in response_text.lower():
                    profile['query_topics'][topic] += 1
                    break

    def _structure_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        """将分析结果结构化为数据库字段"""
        # 计算平均满意度
        avg_satisfaction = (sum(profile['satisfaction_scores']) / len(profile['satisfaction_scores'])
                            if profile['satisfaction_scores'] else None)

        # 确定主要兴趣
        main_interests = sorted(
            profile['interests'].items(),
            key=lambda x: x[1],
            reverse=True
        )[:3]

        # 确定偏好交互模式
        preferred_mode = max(
            profile['preferred_modes'].items(),
            key=lambda x: x[1]
        )[0] if profile['preferred_modes'] else None

        # 确定常讨论主题
        common_topics = sorted(
            profile['query_topics'].items(),
            key=lambda x: x[1],
            reverse=True
        )[:3]

        # 构建结构化画像
        structured = {
            'user_id': profile['user_id'],
            'user_name': profile['user_name'],
            'stat_date': datetime.now().date(),  # 可以考虑添加时区处理
            'interaction_count': profile['interaction_count'],
            'avg_satisfaction': avg_satisfaction,
            'preferred_interaction_mode': preferred_mode,
            'main_interests': ', '.join([i[0] for i in main_interests]) if main_interests else None,
            'common_topics': ', '.join([t[0] for t in common_topics]) if common_topics else None,
            'last_interaction_time': profile['last_interaction_time']
        }

        return structured

    def update_profiles(self, profiles: List[Dict[str, Any]]) -> None:
        """更新用户画像表"""
        if not profiles:
            logger.warning("没有需要更新的用户画像")
            return

        logger.info(f"开始更新{len(profiles)}条用户画像记录...")

        try:
            for profile in profiles:
                self._update_single_profile(profile)
            self.conn.commit()
            logger.info(f"成功更新{len(profiles)}条用户画像记录")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"更新用户画像时出错: {str(e)}")
            raise

    def _update_single_profile(self, profile: Dict[str, Any]) -> None:
        """更新单个用户画像"""
        # 检查用户是否已有记录
        check_query = """
                      SELECT id
                      FROM dwd_user_comprehensive_profile_di_tmp2
                      WHERE user_id = %s
                        AND stat_date = %s
                      """
        self.cursor.execute(check_query, (profile['user_id'], profile['stat_date']))
        exists = self.cursor.fetchone()

        thoughts = f"用户偏好{profile['preferred_interaction_mode']}模式，平均满意度{profile['avg_satisfaction']}"

        if exists:
            # 更新现有记录
            update_query = """
                           UPDATE dwd_user_comprehensive_profile_di_tmp2
                           SET user_name    = %s,
                               hobbies      = %s,
                               thoughts     = %s,
                               updated_time = NOW()
                           WHERE id = %s
                           """
            self.cursor.execute(update_query, (
                profile['user_name'],
                profile['main_interests'],
                thoughts,
                exists['id']
            ))
        else:
            # 插入新记录
            insert_query = """
                           INSERT INTO dwd_user_comprehensive_profile_di_tmp2
                           (user_id, stat_date, user_name, hobbies, thoughts, created_time, updated_time)
                           VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                           """
            self.cursor.execute(insert_query, (
                profile['user_id'],
                profile['stat_date'],
                profile['user_name'],
                profile['main_interests'],
                thoughts
            ))


if __name__ == "__main__":
    # 数据库配置
    db_config = {
        'host': '192.168.10.105',  # 数据库主机
        'user': 'licz.1',  # 数据库用户名
        'password': 'GjFmT5NEiE',  # 数据库密码
        'database': 'sq_liufengdb'  # 数据库名称
    }

    try:
        # 使用上下文管理器确保资源正确释放
        with UserProfileGenerator(db_config) as generator:
            # 分析交互数据并生成画像
            profiles = generator.analyze_interactions()


            if profiles:
                # 更新用户画像表
                generator.update_profiles(profiles)
                print("用户画像更新完成！")
            else:
                print("没有需要更新的用户画像")

    except Exception as e:
        print(f"发生错误: {str(e)}")
        logger.error(f"主程序执行出错: {str(e)}", exc_info=True)
