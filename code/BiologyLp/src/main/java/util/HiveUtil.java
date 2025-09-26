package util;

import config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hive操作工具类
 * 提供Hive数据库连接、SQL执行、结果集处理等功能
 * 基于hive-site.xml和hive-env.sh配置
 */
public class HiveUtil implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(HiveUtil.class);

    private Connection connection;
    private boolean isConnected = false;

    static {
        try {
            Class.forName(AppConfig.HIVE_DRIVER);
            logger.info("Hive JDBC驱动加载成功");
        } catch (ClassNotFoundException e) {
            logger.error("Hive JDBC驱动加载失败", e);
            throw new RuntimeException("无法加载Hive JDBC驱动", e);
        }
    }

    /**
     * 获取Hive连接
     *
     * @return Connection对象
     * @throws SQLException SQL异常
     */
    public Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            try {
                connection = DriverManager.getConnection(
                        AppConfig.HIVE_JDBC_URL,
                        AppConfig.HIVE_USER,
                        AppConfig.HIVE_PASSWORD
                );
                isConnected = true;
                logger.info("Hive连接建立成功: {}", AppConfig.HIVE_JDBC_URL);
            } catch (SQLException e) {
                isConnected = false;
                logger.error("Hive连接建立失败", e);
                throw e;
            }
        }
        return connection;
    }

    /**
     * 检查连接状态
     *
     * @return 是否已连接
     */
    public boolean isConnected() {
        try {
            return isConnected && connection != null && !connection.isClosed();
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * 执行查询SQL
     *
     * @param sql SQL语句
     * @return 查询结果列表
     * @throws SQLException SQL异常
     */
    public List<Map<String, Object>> executeQuery(String sql) throws SQLException {
        logger.info("执行查询SQL: {}", sql);

        List<Map<String, Object>> results = new ArrayList<>();

        try (PreparedStatement statement = getConnection().prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = resultSet.getObject(i);
                    row.put(columnName, value);
                }
                results.add(row);
            }

            logger.info("查询完成，返回{}条记录", results.size());
        } catch (SQLException e) {
            logger.error("查询执行失败: {}", sql, e);
            throw e;
        }

        return results;
    }

    /**
     * 执行更新SQL（INSERT、UPDATE、DELETE、CREATE等）
     *
     * @param sql SQL语句
     * @return 影响的行数
     * @throws SQLException SQL异常
     */
    public int executeUpdate(String sql) throws SQLException {
        logger.info("执行更新SQL: {}", sql);

        try (PreparedStatement statement = getConnection().prepareStatement(sql)) {
            int result = statement.executeUpdate();
            logger.info("更新完成，影响{}行", result);
            return result;
        } catch (SQLException e) {
            logger.error("更新执行失败: {}", sql, e);
            throw e;
        }
    }

    /**
     * 执行SQL语句（自动判断查询或更新）
     *
     * @param sql SQL语句
     * @return 执行结果
     * @throws SQLException SQL异常
     */
    public Object execute(String sql) throws SQLException {
        String trimmedSql = sql.trim().toLowerCase();

        if (trimmedSql.startsWith("select") || trimmedSql.startsWith("show") ||
                trimmedSql.startsWith("describe") || trimmedSql.startsWith("desc")) {
            return executeQuery(sql);
        } else {
            return executeUpdate(sql);
        }
    }

    /**
     * 创建数据库
     *
     * @param databaseName 数据库名称
     * @throws SQLException SQL异常
     */
    public void createDatabase(String databaseName) throws SQLException {
        String sql = "CREATE DATABASE IF NOT EXISTS " + databaseName;
        executeUpdate(sql);
        logger.info("数据库创建成功: {}", databaseName);
    }

    /**
     * 删除数据库
     *
     * @param databaseName 数据库名称
     * @param cascade      是否级联删除
     * @throws SQLException SQL异常
     */
    public void dropDatabase(String databaseName, boolean cascade) throws SQLException {
        String sql = "DROP DATABASE IF EXISTS " + databaseName;
        if (cascade) {
            sql += " CASCADE";
        }
        executeUpdate(sql);
        logger.info("数据库删除成功: {}", databaseName);
    }

    /**
     * 使用数据库
     *
     * @param databaseName 数据库名称
     * @throws SQLException SQL异常
     */
    public void useDatabase(String databaseName) throws SQLException {
        String sql = "USE " + databaseName;
        executeUpdate(sql);
        logger.info("切换到数据库: {}", databaseName);
    }

    /**
     * 显示所有数据库
     *
     * @return 数据库列表
     * @throws SQLException SQL异常
     */
    public List<Map<String, Object>> showDatabases() throws SQLException {
        return executeQuery("SHOW DATABASES");
    }

    /**
     * 显示当前数据库的所有表
     *
     * @return 表列表
     * @throws SQLException SQL异常
     */
    public List<Map<String, Object>> showTables() throws SQLException {
        return executeQuery("SHOW TABLES");
    }

    /**
     * 显示指定数据库的所有表
     *
     * @param databaseName 数据库名称
     * @return 表列表
     * @throws SQLException SQL异常
     */
    public List<Map<String, Object>> showTables(String databaseName) throws SQLException {
        return executeQuery("SHOW TABLES IN " + databaseName);
    }

    /**
     * 描述表结构
     *
     * @param tableName 表名
     * @return 表结构信息
     * @throws SQLException SQL异常
     */
    public List<Map<String, Object>> describeTable(String tableName) throws SQLException {
        return executeQuery("DESCRIBE " + tableName);
    }

    /**
     * 创建外部表
     *
     * @param tableName  表名
     * @param columns    列定义
     * @param location   数据位置
     * @param fileFormat 文件格式
     * @throws SQLException SQL异常
     */
    public void createExternalTable(String tableName, String columns, String location, String fileFormat) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE EXTERNAL TABLE IF NOT EXISTS ").append(tableName);
        sql.append(" (").append(columns).append(") ");
        sql.append("STORED AS ").append(fileFormat);
        sql.append(" LOCATION '").append(location).append("'");

        executeUpdate(sql.toString());
        logger.info("外部表创建成功: {}", tableName);
    }

    /**
     * 创建内部表
     *
     * @param tableName  表名
     * @param columns    列定义
     * @param fileFormat 文件格式
     * @throws SQLException SQL异常
     */
    public void createTable(String tableName, String columns, String fileFormat) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(tableName);
        sql.append(" (").append(columns).append(") ");
        sql.append("STORED AS ").append(fileFormat);

        executeUpdate(sql.toString());
        logger.info("内部表创建成功: {}", tableName);
    }

    /**
     * 删除表
     *
     * @param tableName 表名
     * @throws SQLException SQL异常
     */
    public void dropTable(String tableName) throws SQLException {
        String sql = "DROP TABLE IF EXISTS " + tableName;
        executeUpdate(sql);
        logger.info("表删除成功: {}", tableName);
    }

    /**
     * 插入数据到表
     *
     * @param tableName 表名
     * @param values    值列表
     * @throws SQLException SQL异常
     */
    public void insertIntoTable(String tableName, String values) throws SQLException {
        String sql = "INSERT INTO TABLE " + tableName + " VALUES " + values;
        executeUpdate(sql);
        logger.info("数据插入成功: {}", tableName);
    }

    /**
     * 从HDFS加载数据到表
     *
     * @param tableName 表名
     * @param hdfsPath  HDFS路径
     * @param overwrite 是否覆盖
     * @throws SQLException SQL异常
     */
    public void loadDataFromHdfs(String tableName, String hdfsPath, boolean overwrite) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("LOAD DATA INPATH '").append(hdfsPath).append("' ");
        if (overwrite) {
            sql.append("OVERWRITE ");
        }
        sql.append("INTO TABLE ").append(tableName);

        executeUpdate(sql.toString());
        logger.info("数据加载成功: {} <- {}", tableName, hdfsPath);
    }

    /**
     * 获取表的行数
     *
     * @param tableName 表名
     * @return 行数
     * @throws SQLException SQL异常
     */
    public long getTableRowCount(String tableName) throws SQLException {
        List<Map<String, Object>> results = executeQuery("SELECT COUNT(*) as count FROM " + tableName);
        if (!results.isEmpty()) {
            Object count = results.get(0).get("count");
            return count instanceof Number ? ((Number) count).longValue() : 0L;
        }
        return 0L;
    }

    /**
     * 提交事务
     *
     * @throws SQLException SQL异常
     */
    public void commit() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.commit();
            logger.debug("事务提交成功");
        }
    }

    /**
     * 回滚事务
     *
     * @throws SQLException SQL异常
     */
    public void rollback() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.rollback();
            logger.debug("事务回滚成功");
        }
    }

    /**
     * 设置自动提交
     *
     * @param autoCommit 是否自动提交
     * @throws SQLException SQL异常
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.setAutoCommit(autoCommit);
            logger.debug("自动提交设置为: {}", autoCommit);
        }
    }

    @Override
    public void close() {
        if (connection != null) {
            try {
                connection.close();
                isConnected = false;
                logger.info("Hive连接已关闭");
            } catch (SQLException e) {
                logger.error("关闭Hive连接时发生错误", e);
            }
        }
    }

    /**
     * 测试连接
     *
     * @return 连接是否正常
     */
    public boolean testConnection() {
        try {
            List<Map<String, Object>> result = executeQuery("SELECT 1 as test");
            return !result.isEmpty() && result.get(0).get("test").equals(1);
        } catch (SQLException e) {
            logger.error("连接测试失败", e);
            return false;
        }
    }

    public static void main(String[] args) {
        try (HiveUtil hiveUtil = new HiveUtil()) {
            if (hiveUtil.testConnection()) {
                System.out.println("Hive连接测试成功");

                // 示例操作
//                hiveUtil.createDatabase("test_db");
                hiveUtil.useDatabase("test_db");
//                hiveUtil.createTable("test_table", "id INT, name STRING", "TEXTFILE");
//                hiveUtil.insertIntoTable("test_table", "(1, 'Alice'), (2, 'Bob')");

                List<Map<String, Object>> results = hiveUtil.executeQuery("SELECT * FROM test_table");
                for (Map<String, Object> row : results) {
                    System.out.println(row);
                }

                long rowCount = hiveUtil.getTableRowCount("test_table");
                System.out.println("行数: " + rowCount);

                hiveUtil.dropTable("test_table");
                hiveUtil.dropDatabase("test_db", true);
            } else {
                System.err.println("Hive连接测试失败");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
