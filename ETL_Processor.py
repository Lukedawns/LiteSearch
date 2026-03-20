import logging
import re
import os
from pathlib import Path
# from bs4 import BeautifulSoup
import pymysql
from collections import Counter
import time
from dotenv import load_dotenv


class DocumentProcessor:
    def __init__(self, init=True):
        self.stop_words = {
            "the", "a", "an", "and", "or", "but", "is", "are", "am",
            "in", "on", "at", "to", "for", "with", "of", "this", "that",
            "it", "he", "she", "they", "i", "we", "you"
        }
        if init:
            host = os.getenv('DB_HOST', 'localhost')
            user = os.getenv('DB_USER', 'root')
            password = os.getenv('DB_PASSWORD', '123456')
            db_name = os.getenv('DB_NAME', 'lite_search')

            self.db = pymysql.connect(
                host=host,
                user=user,
                password=password,
                database=db_name,
                autocommit=False
            )
            self.cursor = self.db.cursor()

    def clean_and_tokenize(self, text, use_bigram=True):
        #text = BeautifulSoup(text, "html.parser").get_text(separator=' ')
        text = re.sub(r'<[^>]+>', ' ', text)
        text = text.lower()
        text = re.sub(r'[^a-z0-9\s]', ' ', text)
        tokens = text.split()
        # 过滤停用词
        basic_clean_tokens = [word for word in tokens if word not in self.stop_words]
        final_tokens = list(basic_clean_tokens)

        if use_bigram:
            for i in range(len(basic_clean_tokens) - 1):
                # 将相邻的两个词用空格连起来，当做一个新词
                bigram_word = f"{basic_clean_tokens[i]} {basic_clean_tokens[i + 1]}"
                final_tokens.append(bigram_word)

        return final_tokens

    def load_and_save(self, directory_path, batch_size = 5000):
        directory_path = Path(directory_path)
        if not directory_path.exists():
            error_msg = f"Directory not found: {directory_path.resolve()}"
            logging.error(error_msg)
            raise FileNotFoundError(error_msg)

        # 检查已经存在的数据
        self.cursor.execute("SELECT doc_name FROM documents")
        processed_files = set(row[0] for row in self.cursor.fetchall())
        num_processed_files = len(processed_files)
        logging.info(f"{num_processed_files} files already exist in database.")

        file_count = 0
        batch_documents = []    # 缓存本批次的文件表数据
        batch_inverted_index = []    # 缓存本批次的记录表数据

        self.cursor.execute("SELECT MAX(doc_id) FROM documents")
        max_id_result = self.cursor.fetchone()[0]
        current_doc_id = max_id_result if max_id_result is not None else 0

        need_add = False
        start = time.time()
        for file_path in directory_path.rglob('*.txt'):
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                relative_path = file_path.relative_to(directory_path).as_posix()

                if relative_path in processed_files:
                    continue

                if not need_add:
                    self.temp_close()
                    need_add = True

                content = file.read()
                tokens = self.clean_and_tokenize(content)
                document_token_count = len(tokens)
                current_doc_id += 1
                doc_id = current_doc_id
                # 添加本批次的文件表数据
                batch_documents.append((doc_id, relative_path, document_token_count))
                # 添加本批次的记录表数据
                word_counts = Counter(tokens)
                for word, count in word_counts.items():
                    batch_inverted_index.append((word, doc_id, count))

                file_count += 1

                if file_count % batch_size == 0:
                    self.submit_to_sql(batch_documents, batch_inverted_index)
                    end = time.time()
                    logging.info(f"[{num_processed_files + file_count}] files processed and flushed to MySQL. batch time cost: {round(end - start, 2)}s")
                    # 清空缓存，开始下一批
                    batch_documents = []
                    batch_inverted_index = []
                    start = time.time()

        # 处理最后的剩余部分
        if batch_documents:
            self.submit_to_sql(batch_documents, batch_inverted_index)

        if need_add:
            self.restart()

        logging.info(f"ETL to MySQL completed successfully. {file_count} files added. Total files: {num_processed_files + file_count}")
        return

    def submit_to_sql(self, documents, inverted_index):
        if not documents: return
        # 写入本批次的文档表
        self.cursor.executemany("INSERT IGNORE INTO documents (doc_id, doc_name, word_count) VALUES (%s, %s, %s)", documents)

        # (optional for idempotence)
        # doc_ids = [doc[0] for doc in documents_data]
        # format_strings = ','.join(['%s'] * len(doc_ids))
        # self.cursor.execute(f"DELETE FROM inverted_index WHERE doc_id IN ({format_strings})", tuple(doc_ids))

        # 写入本批次的记录表
        self.cursor.executemany("INSERT INTO inverted_index (keyword, doc_id, tf) VALUES (%s, %s, %s)", inverted_index)

        self.db.commit()

    def temp_close(self):
        logging.info("Temporarily disabling constraints and dropping indexes for bulk load...")
        self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        self.cursor.execute("SET UNIQUE_CHECKS = 0;")

        try:
            self.cursor.execute("ALTER TABLE inverted_index DROP INDEX idx_keyword;")
        except pymysql.MySQLError:
            pass

    def restart(self):
        logging.info(f"Rebuilding indexes...")

        self.cursor.execute("CREATE INDEX idx_keyword ON inverted_index(keyword);")
        self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        self.cursor.execute("SET UNIQUE_CHECKS = 1;")
        self.db.commit()

    def close_connection(self):
        try:
            self.cursor.close()
            self.db.close()
        except Exception as e:
            logging.exception(f"Error closing connection: {e}")

if __name__ == "__main__":
    load_dotenv('.env')
    processor = DocumentProcessor()
    processor.load_and_save("./dataset")
