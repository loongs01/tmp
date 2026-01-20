import os
import sys

# Ensure src is in path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from src.main_system import KnowledgeSystem

def main():
    # Initialize system
    storage_dir = os.path.join(current_dir, "data")
    system = KnowledgeSystem(storage_dir=storage_dir)
    
    # Define output directory
    output_dir = os.path.join(current_dir, "reports")
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "data_sources_analysis.md")
    
    print("Analyzing knowledge base for Data Sources...")
    
    # Queries related to data sources
    queries = [
        "数据源", 
        "系统接入", 
        "SAP", 
        "SRM", 
        "CRM", 
        "PLM",
        "OA",
        "数据库",
        "接口"
    ]
    
    all_results = {}
    
    for q in queries:
        print(f"Querying: {q}")
        results = system.query(q)
        all_results[q] = results
        
    # Generate Report
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# 数据中台项目数据源接入分析报告\n\n")
        f.write("## 1. 检索概览\n")
        f.write(f"基于本地知识库，针对以下关键词进行了检索：{', '.join(queries)}\n\n")
        
        f.write("## 2. 发现的潜在数据源与系统\n")
        
        # Aggregate Graph Results (Entities/Relations)
        f.write("### 知识图谱关联信息\n")
        found_relations = False
        for q, res in all_results.items():
            graph_res = res.get('graph_search', [])
            if graph_res:
                found_relations = True
                f.write(f"**关键词 '{q}' 相关:**\n")
                for item in graph_res:
                    # item format: (node, relation, neighbor)
                    f.write(f"- {item[0]} --[{item[1]}]--> {item[2]}\n")
                f.write("\n")
        
        if not found_relations:
            f.write("（暂未在图谱中发现明确的实体关系，请参考下方文档片段）\n\n")
            
        # Aggregate Vector Search Results (Document Snippets)
        f.write("## 3. 相关文档片段 (RAG Context)\n")
        seen_docs = set()
        
        for q, res in all_results.items():
            vec_res = res.get('vector_search', [])
            if vec_res:
                f.write(f"### 关键词: {q}\n")
                for doc_path, score in vec_res:
                    # We only show the file name and a snippet if possible
                    # Since vector_search currently returns (doc_path, score), we don't have the snippet text directly returned by search()
                    # But we can infer relevance from the filename.
                    
                    filename = os.path.basename(doc_path)
                    if filename not in seen_docs:
                        f.write(f"- **来源文档**: `{filename}` (相关度: {score:.4f})\n")
                        seen_docs.add(filename)
                f.write("\n")
                
    print(f"Analysis complete. Report saved to: {output_file}")

if __name__ == "__main__":
    main()
