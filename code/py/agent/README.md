# Local Knowledge System (Agent)

这是一个完全本地化的知识管理系统，具备知识提取、存储、检索和推理能力。

## 1. 项目结构

```text
d:\note\code\py\agent\
├── src/                        # 核心源代码
│   ├── extractors/             # 信息提取模块
│   │   └── information_extractor.py  # 实体与三元组提取 (Spacy/Regex Fallback)
│   ├── builders/               # 图谱构建模块
│   │   └── graph_builder.py    # 基于 NetworkX 的知识图谱管理 (双向查询)
│   ├── storages/               # 向量存储模块
│   │   └── vector_store.py     # FAISS 向量索引 (Transformers/TF-IDF Fallback)
│   ├── reasoners/              # 推理引擎模块
│   │   └── logic_reasoner.py   # 传递性推理与逻辑分析
│   └── main_system.py          # 系统调度中心 (多格式解析、增量更新)
├── data/                       # 数据持久化目录
│   ├── graph.pkl               # 知识图谱序列化文件
│   ├── vector_index/           # FAISS 索引目录
│   └── processed_files.json    # 已处理文件注册表 (用于增量更新)
├── reports/                    # 归纳结果输出目录
│   ├── data_sources.md         # 已接入数据源清单
│   └── data_sources_analysis.md # 数据中台项目接入分析报告
├── requirements.txt            # 项目依赖
├── test_system.py              # 系统功能测试脚本
├── run_on_documents.py         # 批量文档处理脚本
└── analyze_data_sources.py     # RAG 分析与归纳脚本
```

## 2. 核心功能说明

### 2.1 多格式文档解析
系统支持多种格式的自动化解析与内容提取：
- **文本类**：`.txt`, `.md`
- **结构化**：`.json`, `.csv`
- **办公文档**：`.xlsx` (含损坏自动修复), `.pptx`, `.docx`
- **便携格式**：`.pdf`
- **压缩包**：`.zip` (自动递归解析内部文件)

### 2.2 增量更新与记忆
- **文件注册**：通过 `processed_files.json` 记录已处理文件的绝对路径。
- **智能跳过**：再次运行加载程序时，自动跳过未变动的旧文件，仅处理新增文件。

### 2.3 混合检索 (RAG)
- **语义搜索**：利用向量存储实现基于含义的相关性检索。
- **图谱检索**：利用知识图谱实现基于实体关系的精确查找（支持双向关联查询）。

### 2.4 鲁棒性设计 (Fallback)
系统针对环境依赖做了深度优化，即使高级库加载失败也能运行：
- **提取**：Spacy 失败时自动切换至正则表达式提取。
- **向量**：Sentence-Transformers 失败时自动切换至 Scikit-learn TF-IDF 或词频重叠算法。

### 2.5 推理引擎
- **传递性推理**：能够根据 "A 属于 B, B 属于 C" 推导出 "A 属于 C"。
- **双向关联**：支持查询实体的所有前向和后向关系。

## 3. 使用指南

### 3.1 环境安装
```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

### 3.2 批量索引文档
将文档放入 `d:\note\code\py\document` 后运行：
```bash
python run_on_documents.py
```

### 3.3 运行项目分析
针对特定项目（如数据中台）生成接入分析报告：
```bash
python analyze_data_sources.py
```
结果将保存在 `reports/data_sources_analysis.md`。
