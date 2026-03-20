# LiteSearch
## A Lightweight BM25 Search Engine
基于Python和MySQL的轻量级本地文本搜索引擎。底层实现了文本清洗、倒排索引构建、ETL 批量数据灌入、使用BM25文本相关性打分算法判断相关性。

### 核心特性
- 核心算法升级：使用BM25算法，引入了长文本惩罚因子（$b$）和词频饱和度控制（$k_1$），搜索结果的排序比传统 TF-IDF 更加精准合理
- ETL流水线：针对海量文本，实现了按批次提交的 MySQL 灌库逻辑。在大量写入时自动禁用外键和索引约束，写入完成后重建索引，极大地提升了建库性能。
- 智能摘要提取：实现“最长词汇优先匹配”与“智能边界对齐”，确保摘要中返回的英文单词不会被拦腰截断，并利用正则实现精准高亮。
- 基本Web架构：实现基于FastAPI的后端，可通过自带 Swagger UI (/docs) 接口调用。
- 搜索历史审计：自动异步记录用户的每一次搜索关键词、命中文件、得分及耗时

### 数据集
使用经典的 aclImdb_v1 (Large Movie Review Dataset) 电影评论数据集。

数据规模：共计 125 MB，包含数万篇正向与负向的英文电影评论纯文本。

### 使用方法
需配置.env文件： DB_HOST, DB_USER, DB_PASSWORD, DB_NAME

首先运行SQL_Create.py

之后再通过终端命令行运行uvicorn app:app --reload

服务启动后，在浏览器访问 http://127.0.0.1:8000/docs 即可打开可视化的 API 调试界面。

### 接口概述
POST /api/index：触发 ETL 流水线，读取 dataset/ 目录下的所有文件并建立倒排索引。需首先运行。

GET /api/search?q={keyword}：执行搜索。返回结果包括命中的文档路径、BM25 得分、智能截取的摘要及耗时统计。
