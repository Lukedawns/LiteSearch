import os
import logging
import pymysql
from dotenv import load_dotenv


def init_database():
    host = os.getenv('DB_HOST', 'localhost')
    user = os.getenv('DB_USER', 'root')
    password = os.getenv('DB_PASSWORD', '123456')
    db_name = os.getenv('DB_NAME', 'lite_search')

    db = None
    cursor = None
    # 连接 SQL 服务
    try:
        db = pymysql.connect(
            host=host,
            user=user,
            password=password,
            autocommit=True  # 开启自动提交，建表语句会立刻生效
        )
        cursor = db.cursor()

        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
        cursor.execute(f"USE {db_name};")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                doc_id INT AUTO_INCREMENT PRIMARY KEY,
                doc_name VARCHAR(255) UNIQUE NOT NULL,
                word_count INT
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inverted_index (
                id INT AUTO_INCREMENT PRIMARY KEY,
                keyword VARCHAR(255) NOT NULL,
                doc_id INT NOT NULL,
                tf INT NOT NULL,
                INDEX idx_keyword (keyword),
                FOREIGN KEY (doc_id) REFERENCES documents(doc_id)
            );
        """)

        logging.info(f"Database '{db_name}' and tables initiated successfully.")

    except pymysql.MySQLError as e:
        logging.error(f"Database initiation failed: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'db' in locals():
            db.close()


if __name__ == "__main__":
    load_dotenv()
    init_database()