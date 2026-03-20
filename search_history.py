import logging
import os
import pymysql


class QueryHistory:
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
            autocommit=True
        )
        self.cursor = self.db.cursor()

    def create_history(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS search_logs (
        id INT AUTO_INCREMENT PRIMARY KEY,
        query_words VARCHAR(255) NOT NULL, 
        doc_path VARCHAR(255) NOT NULL,
        score FLOAT,
        context TEXT NOT NULL,
        search_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- 自动记录搜索时间
    );""")

    def insert_history(self, index_data):
        insert_query = """
            INSERT into search_logs (query_words, doc_path, score, context)
            VALUES (%s, %s, %s, %s) 
        """

        self.cursor.executemany(insert_query, index_data)


    def close_connection(self):
        try:
            self.cursor.close()
            self.db.close()
        except Exception as e:
            logging.exception(f"Error closing connection: {e}")
