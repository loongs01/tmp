import pymysql
from datetime import datetime, timedelta
from collections import defaultdict
import spacy

# MySQL 数据库配置

db_config = {
    'host': '192.168.10.105',  # 数据库主机
    'user': 'licz.1',  # 数据库用户名
    'password': 'GjFmT5NEiE',  # 数据库密码
    'database': 'sq_liufengdb'  # 数据库名称
}


def get_db_connection():
    """获取MySQL数据库连接"""
    return pymysql.connect(**db_config)


def analyze_interactions(user_id):
    """分析用户交互数据并生成画像"""
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    # 1. 获取用户基本信息
    cursor.execute("""
                   SELECT DISTINCT user_id, user_name
                   FROM ods_user_interaction_di
                   WHERE user_id = %s
                   """, (user_id,))
    user_info = cursor.fetchone()

    if not user_info:
        return None

    # 2. 分析饮食偏好
    food_keywords = ['食物', '吃', '餐厅', '烹饪', '食谱', '寿司', '披萨', '沙拉']
    dietary_keywords = ['素食', '过敏', '忌口', '饮食限制', '无麸质']
    food_prefs = set()
    dietary_restrictions = set()

    # 3. 分析兴趣爱好
    # hobby_keywords = ['游戏', '读书', '电影', '音乐', '运动', '旅行', '摄影', '艺术']
    hobbies = set()
    favorite_games = set()
    favorite_books = set()
    favorite_movies = set()

    # 4. 分析个人发展
    development_keywords = ['目标', '计划', '梦想', '理想', '成就']
    achievements = set()
    dreams = set()
    ideals = set()
    plans = set()
    goals = set()

    # 5. 其他信息
    thoughts = []
    inspirations = []
    memories = defaultdict(list)
    travel_places = set()
    desired_travel = set()
    color_prefs = set()

    # 查询用户所有交互记录
    cursor.execute("""
                   SELECT query_text, response_text, interaction_time
                   FROM ods_user_interaction_di
                   WHERE user_id = %s
                   ORDER BY interaction_time
                   """, (user_id,))

    interactions = cursor.fetchall()

    for interaction in interactions:
        query = interaction['query_text'].lower()
        response = interaction['response_text']

        # 分析饮食偏好
        if any(kw in query for kw in food_keywords):
            if '寿司' in query:
                food_prefs.add('寿司')
            if '披萨' in query:
                food_prefs.add('披萨')
            if '沙拉' in query:
                food_prefs.add('沙拉')

        if any(kw in query for kw in dietary_keywords):
            if '素食' in query:
                dietary_restrictions.add('素食')
            if '无麸质' in query:
                dietary_restrictions.add('无麸质')

        # 分析兴趣爱好
        if '游戏' in query:
            hobbies.add('游戏')
            if '英雄联盟' in query:
                favorite_games.add('英雄联盟')
            if '原神' in query:
                favorite_games.add('原神')

        if '书' in query or '阅读' in query:
            hobbies.add('阅读')
            if '百年孤独' in query:
                favorite_books.add('百年孤独')
            if '三体' in query:
                favorite_books.add('三体')

        if '电影' in query:
            hobbies.add('电影')
            if '教父' in query:
                favorite_movies.add('教父')
            if '星际穿越' in query:
                favorite_movies.add('星际穿越')

        # 分析个人发展
        if '目标' in query:
            goals.add(response[:100])  # 截取部分作为摘要
        if '计划' in query:
            plans.add(response[:100])
        if '梦想' in query:
            dreams.add(response[:100])

        # 记录想法和灵感
        if '想法' in query or '思考' in query:
            thoughts.append(response[:200])
        if '灵感' in query:
            inspirations.append(response[:200])

        # 分析旅行偏好
        if '旅行' in query or '旅游' in query:
            hobbies.add('旅行')
            if '去过' in query or '去过' in response:
                travel_places.update(extract_places(response))
            if '想去' in query:
                desired_travel.update(extract_places(response))

        # 分析颜色偏好 (假设有相关交互)
        if '颜色' in query or '色彩' in query:
            color_prefs.update(extract_colors(response))

    # 关闭游标和连接
    cursor.close()
    conn.close()

    # 构造返回的用户画像数据
    profile = {
        'user_id': user_id,
        'stat_date': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),  # 昨天
        'user_name': user_info['user_name'],
        'favorite_foods': ', '.join(food_prefs) if food_prefs else None,
        'dietary_restrictions': ', '.join(dietary_restrictions) if dietary_restrictions else None,
        'favorite_colors': ', '.join(color_prefs) if color_prefs else None,
        'hobbies': ', '.join(hobbies) if hobbies else None,
        'favorite_games': ', '.join(favorite_games) if favorite_games else None,
        'favorite_books': ', '.join(favorite_books) if favorite_books else None,
        'favorite_movies': ', '.join(favorite_movies) if favorite_movies else None,
        'achievements': '; '.join(achievements) if achievements else None,
        'dreams': '; '.join(dreams) if dreams else None,
        'ideals': '; '.join(ideals) if ideals else None,
        'plans': '; '.join(plans) if plans else None,
        'goals': '; '.join(goals) if goals else None,
        'thoughts': 'n'.join(thoughts) if thoughts else None,
        'inspirations': 'n'.join(inspirations) if inspirations else None,
        'family_memories': '; '.join(memories.get('family', [])) if memories.get('family') else None,
        'friend_memories': '; '.join(memories.get('friend', [])) if memories.get('friend') else None,
        'colleague_memories': '; '.join(memories.get('colleague', [])) if memories.get('colleague') else None,
        'visited_places': ', '.join(travel_places) if travel_places else None,
        'desired_travel_places': ', '.join(desired_travel) if desired_travel else None
    }

    return profile


def extract_places(text):
    """从文本中提取地点信息(简化版)"""
    places = set()
    # 这里应该使用更复杂的地理实体识别，简化为关键字匹配
    place_keywords = ['日本', '泰国', '意大利', '法国', '美国', '中国', '北京', '上海']
    for kw in place_keywords:
        if kw in text:
            places.add(kw)
    return places


def extract_colors(text):
    """从文本中提取颜色信息"""
    colors = set()
    color_keywords = ['红色', '蓝色', '绿色', '黄色', '黑色', '白色', '紫色']
    for kw in color_keywords:
        if kw in text:
            colors.add(kw)
    return colors


def save_profile_to_dwd(profile):
    """将用户画像保存到DWD表"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # 检查是否已存在该用户当天的记录
    cursor.execute("""
                   SELECT 1
                   FROM dwd_user_comprehensive_profile_di_tmp2
                   WHERE user_id = %s
                     AND stat_date = %s
                   """, (profile['user_id'], profile['stat_date']))

    if cursor.fetchone():
        # 更新现有记录
        sql = """
              UPDATE dwd_user_comprehensive_profile_di_tmp2
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
            profile['user_name'],
            profile['favorite_foods'],
            profile['dietary_restrictions'],
            profile['favorite_colors'],
            profile['hobbies'],
            profile['favorite_games'],
            profile['favorite_books'],
            profile['favorite_movies'],
            profile['achievements'],
            profile['dreams'],
            profile['ideals'],
            profile['plans'],
            profile['goals'],
            profile['thoughts'],
            profile['inspirations'],
            profile['family_memories'],
            profile['friend_memories'],
            profile['colleague_memories'],
            profile['visited_places'],
            profile['desired_travel_places'],
            profile['user_id'],
            profile['stat_date']
        )
    else:
        # 插入新记录
        sql = """
              INSERT INTO dwd_user_comprehensive_profile_di_tmp2 (user_id, stat_date, user_name,
                                                                  favorite_foods, dietary_restrictions, favorite_colors,
                                                                  hobbies, favorite_games, favorite_books,
                                                                  favorite_movies,
                                                                  achievements, dreams, ideals, plans, goals,
                                                                  thoughts, inspirations,
                                                                  family_memories, friend_memories, colleague_memories,
                                                                  visited_places, desired_travel_places,
                                                                  created_time, updated_time)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, NOW(), NOW()) 
              """
        params = (
            profile['user_id'],
            profile['stat_date'],
            profile['user_name'],
            profile['favorite_foods'],
            profile['dietary_restrictions'],
            profile['favorite_colors'],
            profile['hobbies'],
            profile['favorite_games'],
            profile['favorite_books'],
            profile['favorite_movies'],
            profile['achievements'],
            profile['dreams'],
            profile['ideals'],
            profile['plans'],
            profile['goals'],
            profile['thoughts'],
            profile['inspirations'],
            profile['family_memories'],
            profile['friend_memories'],
            profile['colleague_memories'],
            profile['visited_places'],
            profile['desired_travel_places']
        )

    try:
        cursor.execute(sql, params)
        conn.commit()
        # print(f"成功处理用户 {profile['user_id']} 的画像数据")
    except Exception as e:
        conn.rollback()
        print(f"处理用户 {profile['user_id']} 时出错: {str(e)}")
    finally:
        cursor.close()
        conn.close()


def process_all_users():
    """处理所有有交互记录的用户"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # 获取所有有交互记录的用户ID
    cursor.execute("""
                   SELECT DISTINCT user_id
                   FROM ods_user_interaction_di
                   -- WHERE interaction_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                   """)

    user_ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    print(f"找到 {len(user_ids)} 个需要处理的用户")

    for user_id in user_ids:
        profile = analyze_interactions(user_id)
        if profile:
            save_profile_to_dwd(profile)


if __name__ == "__main__":
    process_all_users()
    print("用户画像数据生成完成")
