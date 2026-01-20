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
            matnr,
            idnrk_s,
            idnrk,
            alprf
        FROM ods.ods_sap_erp_substitutmaterials_test
        ORDER BY matnr, idnrk_s, 
            CASE 
                WHEN idnrk = idnrk_s THEN 0
                ELSE alprf 
            END
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

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

    def save_results(self, results):
        """保存结果到数据库"""
        # 创建结果表
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS ods.material_alternative_combinations_test (
            pci_bom_code VARCHAR(100) COMMENT 'PCI-BOM编码',
            replace_rule_code VARCHAR(1000) COMMENT '替代规则编码',
            replace_mat_group_code VARCHAR(2000) COMMENT '替代物料组合编码',
            original_rule_code VARCHAR(1000) COMMENT '原始物料编码',
            original_mat_flag VARCHAR(10) COMMENT '原始物料标识',
            insert_dt DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据更新时间'
        ) ENGINE=OLAP 
        PRIMARY KEY(pci_bom_code, replace_rule_code)
        DISTRIBUTED BY HASH(pci_bom_code) BUCKETS 8 
        PROPERTIES (
            "compression" = "LZ4",
            "enable_persistent_index" = "true",
            "replication_num" = "1"
        )
        """

        self.cursor.execute("DROP TABLE IF EXISTS ods.material_alternative_combinations_test")
        self.cursor.execute(create_table_sql)

        # 插入数据
        insert_sql = """
        INSERT INTO ods.material_alternative_combinations_test 
        (pci_bom_code, replace_rule_code, replace_mat_group_code, original_rule_code, original_mat_flag)
        VALUES (%s, %s, %s, %s, %s)
        """

        batch_data = []
        for result in results:
            batch_data.append((
                result['pci_bom_code'],
                result['replace_rule_code'],
                result['replace_mat_group_code'],
                result['original_rule_code'],
                result['original_mat_flag']
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

            # 保存结果
            print("正在保存结果到数据库...")
            self.save_results(results)
            print("结果保存完成")

            # 显示前10条结果
            print("\n前10条结果:")
            print("PCI-BOM编码 | 替代规则编码 | 替代物料组合编码 | 原始物料编码 | 原始物料标识")
            print("-" * 80)
            for i, result in enumerate(results[:10]):
                print(f"{result['pci_bom_code']} | {result['replace_rule_code']} | "
                      f"{result['replace_mat_group_code']} | {result['original_rule_code']} | "
                      f"{result['original_mat_flag']}")

        except Exception as e:
            print(f"处理过程中发生错误: {str(e)}")
        finally:
            self.disconnect()
            print("数据库连接已关闭")


# 配置信息
STARROCKS_CONFIG = {
    "host": "10.2.8.36",
    "port": 9030,
    "user": "root",
    "password": "iPXE83EEZSrOBUfe",
    "database": "test",
    "charset": "utf8"
}

# 运行程序
if __name__ == "__main__":
    generator = MaterialAlternativeGenerator(STARROCKS_CONFIG)
    generator.run()