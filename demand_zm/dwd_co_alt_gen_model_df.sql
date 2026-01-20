import mysql.connector
import itertools

class MaterialAlternativeGenerator:
    def __init__(self, config):
        self.config = config
        self.connection = None
        self.cursor = None

    def connect(self):
        """连接StarRocks数据库"""
        self.connection = mysql.connector.connect(
            host=self.config['host'],
            port=self.config['port'],
            user=self.config['user'],
            password=self.config['password'],
            database=self.config['database'],
            charset=self.config['charset']
        )
        self.cursor = self.connection.cursor(dictionary=True)

    def disconnect(self):
        """断开数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def get_material_data(self):
        """从数据库获取替代物料数据"""
        query = """
        SELECT 
            st.matnr,
            st.idnrk_s,
            st.idnrk,
            st.alprf
        FROM ods.ods_aps_escp_inf_datacenter_o_substitutmaterials_df as st
        ORDER BY st.matnr, st.idnrk_s, 
            CASE 
                WHEN st.idnrk = st.idnrk_s THEN 0
                ELSE st.alprf 
            END
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_material_classification(self, material_codes):
        """批量获取物料的三级分类信息"""
        if not material_codes:
            return {}
        
        classification_info = {}
        
        # 分批查询，避免SQL过长
        batch_size = 1000
        for i in range(0, len(material_codes), batch_size):
            batch = material_codes[i:i + batch_size]
            placeholders = ', '.join(['%s'] * len(batch))
            
            query = f"""
            SELECT mat_code, three_lv_class 
            FROM dim.dim_mat_info_df 
            WHERE mat_code IN ({placeholders})
            """
            
            self.cursor.execute(query, batch)
            results = self.cursor.fetchall()
            
            for row in results:
                classification_info[row['mat_code']] = row['three_lv_class'] or ''
        
        return classification_info

    def process_material_data(self, data):
        """处理物料数据，构建数据结构"""
        result = {}

        for row in data:
            matnr = row['matnr']
            std_mat = row['idnrk_s']
            alt_mat = row['idnrk']

            if matnr not in result:
                result[matnr] = {}

            if std_mat not in result[matnr]:
                result[matnr][std_mat] = []

            # 添加替代物料（包括自身）
            if alt_mat not in result[matnr][std_mat]:
                result[matnr][std_mat].append(alt_mat)

        return result

    def parse_replace_group(self, replace_group_code):
        """解析替代物料组合编码，提取替换物料对"""
        # 样例: M03:M03-1#M04:M04-1#M05:M05-1
        # 提取标准物料和替换物料对
        replace_pairs = []
        
        if not replace_group_code:
            return replace_pairs
            
        parts = replace_group_code.split('#')
        for part in parts:
            if ':' in part:
                std_mat, replace_mat = part.split(':', 1)
                replace_pairs.append({
                    'std_mat': std_mat,
                    'replace_mat': replace_mat,
                    'pair_str': f"{std_mat}:{replace_mat}"
                })
        
        return replace_pairs

    def calculate_classification_fields(self, replace_group_code, classification_info):
        """根据替代物料组合编码计算分类字段"""
        # 解析替换物料对
        replace_pairs = self.parse_replace_group(replace_group_code)
        
        # 初始化分类字段
        light_parts = []
        ic_parts = []
        power_parts = []
        card_parts = []
        
        # 遍历替换物料对，检查分类
        for pair in replace_pairs:
            std_mat = pair['std_mat']
            replace_mat = pair['replace_mat']
            
            # 只有在物料被替换时才记录（标准物料 ≠ 替换物料）
            if std_mat != replace_mat:
                class_type = classification_info.get(replace_mat, '')
                
               # if class_type == '灯珠':
               #     light_parts.append(pair['pair_str'])
               # elif class_type == '恒流IC':
               #     ic_parts.append(pair['pair_str'])
               # elif class_type == '电源':
               #     power_parts.append(pair['pair_str'])
               # elif class_type == '接收卡':
               #     card_parts.append(pair['pair_str'])
               #    
                if class_type == 'LED':                  # “灯珠”改为“LED”  CHAGNE BY LXL 20260109 
                    light_parts.append(pair['pair_str'])
                elif class_type == '恒流驱动IC':           # “恒流IC”改为“恒流驱动IC”  CHAGNE BY LXL 20260109 
                    ic_parts.append(pair['pair_str'])
                elif class_type == '开关电源':             # “电源”改为“开关电源”  CHAGNE BY LXL 20260109 
                    power_parts.append(pair['pair_str'])
                elif class_type == '接收卡':
                    card_parts.append(pair['pair_str']) 
        
        # 拼接结果
        light_mat_code = '#'.join(light_parts) if light_parts else ''
        ic_mat_code = '#'.join(ic_parts) if ic_parts else ''
        power_mat_code = '#'.join(power_parts) if power_parts else ''
        card_mat_code = '#'.join(card_parts) if card_parts else ''
        
        return light_mat_code, ic_mat_code, power_mat_code, card_mat_code

    def generate_combinations(self, material_dict):
        """生成所有可能的替代组合"""
        all_combinations = []

        for matnr, std_materials in material_dict.items():
            # 获取标准物料列表并排序
            std_list = sorted(std_materials.keys())

            # 构建原始规则
            original_rule = f"{matnr}#{'#'.join(std_list)}"

            # 构建标准物料组合编码
            standard_group = '#'.join([f"{mat}:{mat}" for mat in std_list])

            # 添加原始物料组合（标识为Y）
            all_combinations.append({
                'pci_bom_code': matnr,
                'replace_rule_code': original_rule,
                'replace_mat_group_code': standard_group,
                'original_rule_code': original_rule,
                'original_mat_flag': 'Y'
            })

            # 为每个标准物料生成替代选项列表
            alternatives_lists = []
            for std_mat in std_list:
                alternatives_lists.append(std_materials[std_mat])

            # 使用itertools.product生成所有可能的组合
            product_combinations = list(itertools.product(*alternatives_lists))

            # 处理每个组合，跳过原始物料组合（已在上面添加）
            for combination in product_combinations:
                # 检查是否是原始物料组合
                if list(combination) == std_list:
                    continue

                # 构建替代规则编码
                replace_rule = f"{matnr}#{'#'.join(combination)}"

                # 构建替代物料组合编码
                replace_group_parts = []
                for i, selected_mat in enumerate(combination):
                    std_mat = std_list[i]
                    replace_group_parts.append(f"{std_mat}:{selected_mat}")

                replace_group = '#'.join(replace_group_parts)

                all_combinations.append({
                    'pci_bom_code': matnr,
                    'replace_rule_code': replace_rule,
                    'replace_mat_group_code': replace_group,
                    'original_rule_code': original_rule,
                    'original_mat_flag': ''
                })

        return all_combinations

    def process_classification_fields(self, results):
        """处理分类字段：根据replace_mat_group_code计算四个分类字段"""
        print("正在计算分类字段...")
        
        # 收集所有出现的物料编码
        all_materials = set()
        
        # 第一步：收集所有需要查询的物料编码
        for result in results:
            replace_pairs = self.parse_replace_group(result['replace_mat_group_code'])
            for pair in replace_pairs:
                all_materials.add(pair['replace_mat'])
        
        print(f"共发现 {len(all_materials)} 个不同的替换物料编码需要查询分类")
        
        # 第二步：批量查询物料分类信息
        classification_info = self.get_material_classification(list(all_materials))
        print(f"成功获取到 {len(classification_info)} 个物料的分类信息")
        
        # 第三步：为每个结果计算分类字段
        for result in results:
            replace_group = result['replace_mat_group_code']
            
            # 计算分类字段
            light_mat_code, ic_mat_code, power_mat_code, card_mat_code = self.calculate_classification_fields(
                replace_group, classification_info
            )
            
            # 更新结果
            result['light_mat_code'] = light_mat_code
            result['ic_mat_code'] = ic_mat_code
            result['power_mat_code'] = power_mat_code
            result['card_mat_code'] = card_mat_code
        
        return results

    def save_results(self, results):
        """保存结果到数据库"""
        # 清空目标表
        self.cursor.execute("truncate table dwd.dwd_co_alt_gen_model_df")
        self.cursor.execute("SET enable_insert_strict = false")

        # 插入数据
        insert_sql = """
        INSERT INTO dwd.dwd_co_alt_gen_model_df 
        (pci_bom_code, replace_rule_code, replace_mat_group_code, original_rule_code, 
         original_mat_flag, light_mat_code, ic_mat_code, power_mat_code, card_mat_code)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        batch_data = []
        for result in results:
            batch_data.append((
                result['pci_bom_code'],
                result['replace_rule_code'],
                result['replace_mat_group_code'],
                result['original_rule_code'],
                result['original_mat_flag'],
                result['light_mat_code'] or None,
                result['ic_mat_code'] or None,
                result['power_mat_code'] or None,
                result['card_mat_code'] or None
            ))

        # 分批插入，避免数据量过大
        batch_size = 1000
        for i in range(0, len(batch_data), batch_size):
            batch = batch_data[i:i + batch_size]
            self.cursor.executemany(insert_sql, batch)
            self.connection.commit()
            print(f"已插入 {i + len(batch)} 条记录")

    def run(self):
        """主运行函数"""
        try:
            # 连接数据库
            self.connect()
            print("数据库连接成功")

            # 获取数据
            print("正在获取物料数据...")
            raw_data = self.get_material_data()
            print(f"获取到 {len(raw_data)} 条原始记录")

            # 处理数据
            print("正在处理物料数据...")
            material_dict = self.process_material_data(raw_data)

            # 生成组合
            print("正在生成替代组合...")
            results = self.generate_combinations(material_dict)
            print(f"生成了 {len(results)} 个替代组合")

            # 处理分类字段
            results = self.process_classification_fields(results)

            # 保存结果
            print("正在保存结果到数据库...")
            self.save_results(results)
            print("结果保存完成")

            # 显示前10条结果（包含分类字段）
            print("\n前10条结果（包含分类字段）:")
            print("PCI-BOM编码 | 替代组合编码 | 灯珠物料 | 恒流IC | 电源 | 接收卡 | 原始标识")
            print("-" * 120)
            
            for i, result in enumerate(results[:10]):
                # 缩短显示，便于查看
                replace_group_display = result['replace_mat_group_code']
                if len(replace_group_display) > 30:
                    replace_group_display = replace_group_display[:27] + "..."
                
                light_display = result['light_mat_code']
                if light_display and len(light_display) > 15:
                    light_display = light_display[:12] + "..."
                
                ic_display = result['ic_mat_code']
                if ic_display and len(ic_display) > 10:
                    ic_display = ic_display[:7] + "..."
                
                power_display = result['power_mat_code']
                if power_display and len(power_display) > 10:
                    power_display = power_display[:7] + "..."
                
                card_display = result['card_mat_code']
                if card_display and len(card_display) > 10:
                    card_display = card_display[:7] + "..."
                
                print(f"{result['pci_bom_code']:10} | {replace_group_display:30} | "
                      f"{light_display or '无':15} | "
                      f"{ic_display or '无':10} | "
                      f"{power_display or '无':10} | "
                      f"{card_display or '无':10} | "
                      f"{result['original_mat_flag']:3}")

            # 显示分类统计和示例
            print("\n分类字段统计和示例:")
            light_results = [r for r in results if r['light_mat_code']]
            ic_results = [r for r in results if r['ic_mat_code']]
            power_results = [r for r in results if r['power_mat_code']]
            card_results = [r for r in results if r['card_mat_code']]
            
            print(f"包含灯珠物料: {len(light_results)} 条")
            if light_results:
                print(f"  示例: {light_results[0]['light_mat_code']}")
            
            print(f"包含恒流IC物料: {len(ic_results)} 条")
            if ic_results:
                print(f"  示例: {ic_results[0]['ic_mat_code']}")
            
            print(f"包含电源物料: {len(power_results)} 条")
            if power_results:
                print(f"  示例: {power_results[0]['power_mat_code']}")
            
            print(f"包含接收卡物料: {len(card_results)} 条")
            if card_results:
                print(f"  示例: {card_results[0]['card_mat_code']}")

        except Exception as e:
            print(f"处理过程中发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
        finally:
            self.disconnect()
            print("数据库连接已关闭")


# 配置信息
# STARROCKS_CONFIG = {
#     "host": "10.2.8.36",
#     "port": 9030,
#     "user": "root",
#     "password": "iPXE83EEZSrOBUfe",
#     "database": "test",
#     "charset": "utf8"
# }


# 运行程序
if __name__ == "__main__":
    import sys
    helper_path = '/data/dolphinscheduler/resources'
    sys.path.insert(0,helper_path)
    import config
    # 获取配置信息
    STARROCKS_CONFIG=config.sr_config()
    generator = MaterialAlternativeGenerator(STARROCKS_CONFIG)
    generator.run()