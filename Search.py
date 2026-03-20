import logging
import os
import pymysql
import math
import heapq
from collections import defaultdict


class Search:
    def __init__(self):
        host = os.getenv('DB_HOST', 'localhost')
        user = os.getenv('DB_USER', 'root')
        password = os.getenv('DB_PASSWORD', '123456')
        db_name = os.getenv('DB_NAME', 'lite_search')

        self.db = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
            autocommit = True
        )
        self.cursor = self.db.cursor(pymysql.cursors.DictCursor)

    def search(self, query_words, top_k=10, k1=1.5, b=0.75):
        self.cursor.execute("SELECT COUNT(*) as total FROM documents")
        total_docs = self.cursor.fetchone()['total']
        if total_docs == 0:
            return []

        scores = defaultdict(float)
        self.cursor.execute("SELECT AVG(word_count) as avgdl FROM documents")
        avgdl = float(self.cursor.fetchone()['avgdl'])

        for word in query_words:
            word = word.lower()
            query = """
                SELECT i.doc_id, d.doc_name, i.tf, d.word_count
                FROM inverted_index i
                JOIN documents d 
                ON i.doc_id = d.doc_id
                WHERE i.keyword = %s
            """
            self.cursor.execute(query, (word,))
            results = self.cursor.fetchall()

            if not results:
                continue

            # results: [{'doc_id': 1, 'doc_name': 'text.txt', 'tf': 5.0}, {...}, {...}, ...]
            # df (包含该词的文档总数)
            df = len(results)
            # 计算 IDF
            idf = math.log((total_docs - df + 0.5)/(df + 0.5) + 1)

            # 累加文档的BM25得分
            for row in results:
                doc_name = row['doc_name']
                tf = row['tf']
                D = row['word_count']
                BM25 = idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * D / avgdl))
                scores[doc_name] += BM25

        if not scores:
            return []

        # 使用最小堆维护 Top-K
        min_heap = []
        for doc_name, total_score in scores.items():
            heapq.heappush(min_heap, (total_score, doc_name))
            if len(min_heap) > top_k:
                heapq.heappop(min_heap)

        results_list = [(doc_name, round(score, 4)) for score, doc_name in min_heap]
        results_list.sort(key=lambda x: x[1], reverse=True)
        return results_list

    def close_connection(self):
        try:
            self.cursor.close()
            self.db.close()
        except Exception as e:
            logging.exception(f"Error closing connection: {e}")


if __name__ == "__main__":
    print(Search().search("Hello"))