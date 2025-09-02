from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# es = Elasticsearch(
#     ["https://node1:9200", "https://node2:9200"],  # 集群多节点
#     basic_auth=("elastic", "password"),
#     verify_certs=True,
#     ca_certs="/path/to/ca.crt",  # 自定义 CA 证书
#     timeout=30,  # 请求超时（秒）
#     max_retries=3,  # 最大重试次数
#     retry_on_timeout=True,  # 超时后重试
# )

es = Elasticsearch(
    "http://localhost:9200",
    basic_auth=("lee", "lcz0205"),
    verify_certs=False,  # 开发环境禁用证书验证
)

index_name = "products"

# 定义索引映射（可选）
index_mapping = {
    "mappings": {
        "properties": {
            "name": {"type": "text"},
            "price": {"type": "float"},
            "stock": {"type": "integer"},
            "created_at": {"type": "date"}
        }
    }
}


def create_index_if_not_exists():
    """创建索引（如果不存在）"""
    try:
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name, body=index_mapping)
            logger.info(f"Index '{index_name}' created.")
        else:
            logger.info(f"Index '{index_name}' already exists.")
    except Exception as e:
        logger.error(f"创建索引失败: {e}")
        raise


def insert_single_documents():
    """插入单个文档"""
    try:
        # 插入文档1
        doc1 = {
            "name": "Laptop",
            "price": 999.99,
            "stock": 10,
            "created_at": "2025-07-01"
        }
        response1 = es.index(index=index_name, document=doc1)
        logger.info(f"Document 1 created with ID: {response1['_id']}")

        # 插入文档2
        doc2 = {"name": "Smartphone", "price": 699.999}
        response2 = es.index(index=index_name, document=doc2)
        logger.info(f"Document 2 created with ID: {response2['_id']}")

        return response1['_id'], response2['_id']
    except Exception as e:
        logger.error(f"插入文档失败: {e}")
        raise


def bulk_operations():
    """批量操作 - 移除删除操作以避免文档丢失"""
    try:
        # 准备批量数据 - 只包含插入/更新操作，移除删除操作
        actions = [
            {"_index": index_name, "_id": 200, "_source": {"name": "Tablet", "price": 399.99}},
            {"_index": index_name, "_id": 201, "_source": {"name": "Monitor", "price": 199.99}},
            # 移除删除操作: {"_op_type": "delete", "_index": index_name, "_id": 200}
        ]

        # 执行批量操作
        success, failed = bulk(es, actions)
        logger.info(f"Bulk operation completed - Success: {success}, Failed: {failed}")

        if failed:
            logger.warning(f"批量操作中有失败的项目: {failed}")

    except Exception as e:
        logger.error(f"批量操作失败: {e}")
        raise


def verify_documents():
    """验证文档是否存在"""
    try:
        # 刷新索引确保数据可见
        es.indices.refresh(index=index_name)
        logger.info("Index refreshed")

        # 查询所有文档 - 增加size参数以显示所有文档
        query = {
            "query": {"match_all": {}},
            "sort": [{"price": "desc"}],  # 按价格降序
            "size": 100  # 增加返回文档数量，确保显示所有文档
        }

        response = es.search(index=index_name, body=query)
        total_hits = response["hits"]["total"]["value"]

        logger.info(f"Total documents in index: {total_hits}")
        logger.info(f"Documents returned in this query: {len(response['hits']['hits'])}")

        # 显示所有文档
        for i, hit in enumerate(response["hits"]["hits"], 1):
            doc_id = hit["_id"]
            source = hit["_source"]
            logger.info(f"Document {i}: ID={doc_id}, Content={source}")

        # 特别检查ID为201的文档
        try:
            doc_201 = es.get(index=index_name, id=201)
            logger.info(f"Document 201 exists: {doc_201['_source']}")

            # 检查ID为201的文档在查询结果中的位置
            found_in_results = False
            for i, hit in enumerate(response["hits"]["hits"]):
                if hit["_id"] == "201":
                    logger.info(f"Document 201 found in query results at position {i + 1}")
                    found_in_results = True
                    break

            if not found_in_results:
                logger.warning("Document 201 exists but not found in query results (may be due to pagination)")

        except Exception as e:
            logger.warning(f"Document 201 not found: {e}")

        # 额外查询：专门查找ID为201的文档
        specific_query = {
            "query": {
                "term": {"_id": "201"}
            }
        }

        specific_response = es.search(index=index_name, body=specific_query)
        if specific_response["hits"]["total"]["value"] > 0:
            logger.info(f"Document 201 found via specific query: {specific_response['hits']['hits'][0]['_source']}")
        else:
            logger.warning("Document 201 not found via specific query")

    except Exception as e:
        logger.error(f"验证文档失败: {e}")
        raise


def main():
    """主函数"""
    try:
        # 1. 创建索引
        create_index_if_not_exists()

        # 2. 插入单个文档
        insert_single_documents()

        # 3. 执行批量操作
        bulk_operations()

        # 4. 验证文档
        verify_documents()

    except Exception as e:
        logger.error(f"脚本执行失败: {e}")
        raise


if __name__ == "__main__":
    main()
