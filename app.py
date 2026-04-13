import os
import time
import logging
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from dotenv import load_dotenv
# from fastapi.middleware.cors import CORSMiddleware

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(BASE_DIR, '.env')
load_dotenv(env_path, override=True)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info(f"Current database: {os.getenv('DB_NAME')}")

from ETL_Processor import DocumentProcessor
from Search import Search
from snippet import generate_snippet
from search_history import QueryHistory

app = FastAPI(
    title="LiteSearch API",
    description="Lite Local Text Search Engine",
    version="1.0.1"
)

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],  # 允许所有域名访问
#     allow_credentials=True,
#     allow_methods=["*"],  # 允许所有方法
#     allow_headers=["*"],  # 允许所有请求头
# )

dataset_name = 'dataset'
dataset_path = os.path.join(BASE_DIR, dataset_name)

text_processor = DocumentProcessor()
searcher = Search()
history = QueryHistory()
history.create_history()

class SearchResultItem(BaseModel):
    doc_name: str
    score: float
    snippet: str

class SearchResponse(BaseModel):
    keyword: str
    time_cost_seconds: float
    results: list[SearchResultItem]

@app.get("/api/search", response_model=SearchResponse)
def perform_search(q: str = Query(..., description="Search query keywords")):
    if not q or not q.strip():
        raise HTTPException(status_code=400, detail="Search query cannot be empty")

    start = time.time()
    keyword = q.strip()

    processed_keyword = text_processor.clean_and_tokenize(keyword)
    results = searcher.search(processed_keyword)

    response_data = []
    log_list = []

    # 封装结果并生成高亮摘要
    for doc_name, score in results:
        context = generate_snippet(doc_name, processed_keyword, dataset_path)
        if context is None:
            continue

        # 追加到返回给前端的列表中
        response_data.append(SearchResultItem(
            doc_name=doc_name,
            score=score,
            snippet=context
        ))

        # 准备写入历史记录的数据
        log_list.append((keyword, doc_name, score, context))

    # 记录到数据库
    if log_list:
        history.insert_history(log_list)

    end = time.time()
    time_cost = round(end - start, 3)
    logging.info(f"API Search completed for '{keyword}', time cost: {time_cost}s")

    return SearchResponse(
        keyword=keyword,
        time_cost_seconds=time_cost,
        results=response_data
    )


@app.post("/api/index")
def build_index():
    try:
        logging.info("API trigger: Starting ETL process...")
        start = time.time()
        text_processor.load_and_save(dataset_path, batch_size=10000)
        end = time.time()
        return {
            "status": "success",
            "message": "Index built successfully.",
            "time_cost_seconds": round(end - start, 2)
        }
    except FileNotFoundError as e:
        logging.error(f"API trigger ETL failed: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logging.exception(f"API trigger ETL failed with unknown error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error during indexing.")