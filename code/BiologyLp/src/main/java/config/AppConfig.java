package config;

public class AppConfig {
    // MySQL 配置
    public static final String MYSQL_URL = "jdbc:mysql://192.168.10.103:3306/sqcode?useSSL=false&characterEncoding=utf8";
    public static final String MYSQL_USER = "sqcode";
    public static final String MYSQL_PASSWORD = "rSW5Fezbw7GsLLYD";

    // Hadoop 配置
    public static final String HADOOP_HDFS_URI = "hdfs://nameservice1";
    public static final String HADOOP_USER = "root";

    // 分词相关配置
    public static final String DICT_PATH = "resources/dict/";
    public static final String STOPWORDS_PATH = "resources/stopwords.txt";

    // FastText 模型路径
    public static final String FASTTEXT_MODEL_PATH = "resources/model/cc.zh.300.bin";

    // ICD11 相关表名
    public static final String ICD11_CODE_TABLE = "c_icd11_code";
    public static final String ICD11_SIMILAR_TABLE = "c_icd11_similar";
    public static final String ICD11_OTH_CODE_TABLE = "c_icd11_oth_code";

    // Hive 配置
    public static final String HIVE_JDBC_URL = "jdbc:hive2://liuf3:10000/default";
    public static final String HIVE_USER = "hive";
    public static final String HIVE_PASSWORD = "hive";
    public static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    public static final String HIVE_METASTORE_URI = "thrift://liuf1:9083";
    public static final String HIVE_WAREHOUSE_DIR = "/user/hive/warehouse";
}