package org.example;

import util.HiveUtil;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * HiveUtil使用示例
 * 演示如何使用HiveUtil进行各种Hive操作
 */
public class HiveUtilDemo {
    
    public static void main(String[] args) {
        // 使用try-with-resources确保连接正确关闭
        try (HiveUtil hiveUtil = new HiveUtil()) {
            
            // 1. 测试连接
            System.out.println("=== 测试Hive连接 ===");
            if (hiveUtil.testConnection()) {
                System.out.println("✓ Hive连接测试成功");
            } else {
                System.out.println("✗ Hive连接测试失败");
                return;
            }
            
            // 2. 显示所有数据库
            System.out.println("\n=== 显示所有数据库 ===");
            List<Map<String, Object>> databases = hiveUtil.showDatabases();
            for (Map<String, Object> db : databases) {
                System.out.println("数据库: " + db.get("database_name"));
            }
            
            // 3. 创建测试数据库
            System.out.println("\n=== 创建测试数据库 ===");
            String testDb = "test_biology_db";
            hiveUtil.createDatabase(testDb);
            System.out.println("✓ 数据库创建成功: " + testDb);
            
            // 4. 使用测试数据库
            System.out.println("\n=== 切换到测试数据库 ===");
            hiveUtil.useDatabase(testDb);
            System.out.println("✓ 切换到数据库: " + testDb);
            
            // 5. 创建测试表
            System.out.println("\n=== 创建测试表 ===");
            String tableName = "biology_data";
            String columns = "id INT, name STRING, description STRING, create_time TIMESTAMP";
            hiveUtil.createTable(tableName, columns, "TEXTFILE");
            System.out.println("✓ 表创建成功: " + tableName);
            
            // 6. 插入测试数据
            System.out.println("\n=== 插入测试数据 ===");
            String values = "(1, '细胞生物学', '研究细胞结构和功能的学科', '2024-01-01 10:00:00'), " +
                           "(2, '分子生物学', '研究生物大分子结构和功能的学科', '2024-01-01 11:00:00'), " +
                           "(3, '遗传学', '研究遗传现象和规律的学科', '2024-01-01 12:00:00')";
            hiveUtil.insertIntoTable(tableName, values);
            System.out.println("✓ 测试数据插入成功");
            
            // 7. 查询数据
            System.out.println("\n=== 查询数据 ===");
            List<Map<String, Object>> results = hiveUtil.executeQuery("SELECT * FROM " + tableName);
            for (Map<String, Object> row : results) {
                System.out.printf("ID: %s, 名称: %s, 描述: %s, 创建时间: %s%n",
                    row.get("id"), row.get("name"), row.get("description"), row.get("create_time"));
            }
            
            // 8. 获取表行数
            System.out.println("\n=== 获取表行数 ===");
            long rowCount = hiveUtil.getTableRowCount(tableName);
            System.out.println("表 " + tableName + " 共有 " + rowCount + " 行数据");
            
            // 9. 显示表结构
            System.out.println("\n=== 显示表结构 ===");
            List<Map<String, Object>> tableInfo = hiveUtil.describeTable(tableName);
            for (Map<String, Object> column : tableInfo) {
                System.out.printf("列名: %s, 类型: %s, 注释: %s%n",
                    column.get("col_name"), column.get("data_type"), column.get("comment"));
            }
            
            // 10. 显示当前数据库的所有表
            System.out.println("\n=== 显示当前数据库的所有表 ===");
            List<Map<String, Object>> tables = hiveUtil.showTables();
            for (Map<String, Object> table : tables) {
                System.out.println("表名: " + table.get("tab_name"));
            }
            
            // 11. 演示复杂查询
            System.out.println("\n=== 复杂查询示例 ===");
            List<Map<String, Object>> complexResults = hiveUtil.executeQuery(
                "SELECT name, description FROM " + tableName + " WHERE id > 1 ORDER BY name"
            );
            for (Map<String, Object> row : complexResults) {
                System.out.printf("名称: %s, 描述: %s%n", row.get("name"), row.get("description"));
            }
            
            // 12. 清理测试数据
            System.out.println("\n=== 清理测试数据 ===");
            hiveUtil.dropTable(tableName);
            System.out.println("✓ 测试表删除成功");
            
            hiveUtil.dropDatabase(testDb, true);
            System.out.println("✓ 测试数据库删除成功");
            
        } catch (SQLException e) {
            System.err.println("Hive操作发生错误: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("程序执行发生错误: " + e.getMessage());
            e.printStackTrace();
        }
        
        System.out.println("\n=== HiveUtil演示完成 ===");
    }
    
    /**
     * 演示外部表创建和数据加载
     */
    public static void demonstrateExternalTable() {
        try (HiveUtil hiveUtil = new HiveUtil()) {
            
            // 创建外部表
            String externalTableName = "external_biology_data";
            String columns = "id INT, name STRING, description STRING";
            String hdfsLocation = "/user/hive/warehouse/external_biology_data";
            String fileFormat = "TEXTFILE";
            
            hiveUtil.createExternalTable(externalTableName, columns, hdfsLocation, fileFormat);
            System.out.println("外部表创建成功: " + externalTableName);
            
            // 从HDFS加载数据（假设数据文件已存在于HDFS）
            String hdfsDataPath = "/user/hive/warehouse/biology_data.txt";
            hiveUtil.loadDataFromHdfs(externalTableName, hdfsDataPath, false);
            System.out.println("数据加载成功");
            
        } catch (SQLException e) {
            System.err.println("外部表操作发生错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 演示事务操作
     */
    public static void demonstrateTransaction() {
        try (HiveUtil hiveUtil = new HiveUtil()) {
            
            // 关闭自动提交
            hiveUtil.setAutoCommit(false);
            
            try {
                // 执行多个操作
                hiveUtil.executeUpdate("INSERT INTO test_table VALUES (1, 'test1')");
                hiveUtil.executeUpdate("INSERT INTO test_table VALUES (2, 'test2')");
                
                // 提交事务
                hiveUtil.commit();
                System.out.println("事务提交成功");
                
            } catch (SQLException e) {
                // 回滚事务
                hiveUtil.rollback();
                System.out.println("事务回滚成功");
                throw e;
            }
            
        } catch (SQLException e) {
            System.err.println("事务操作发生错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
