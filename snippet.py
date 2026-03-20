import re
from pathlib import Path
from bs4 import BeautifulSoup
import logging

def generate_snippet(doc_name, query_words, data_dir, window=40):
    file_path = Path(data_dir) / doc_name

    if not file_path.exists():
        logging.error("File not exists.")
        return None

    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            raw_text = f.read()
    except Exception as e:
        logging.exception(f"Snippet Generation Failed: Cannot read file {doc_name}. Error: {e}")
        return None

    #raw_text = BeautifulSoup(raw_text, "html.parser").get_text(separator=' ')
    raw_text = re.sub(r'<[^>]+>', ' ', raw_text)
    lower_text = raw_text.lower()

    # 【核心架构优化：最长匹配优先】
    # 将搜索词汇按长度从大到小排序。例如：["data science", "science", "data"]
    sorted_query_words = sorted(query_words, key=len, reverse=True)
    best_match_index = -1
    matched_word_len = 0

    # 遍历排序后的词汇，寻找最具价值的锚点
    for word in sorted_query_words:
        # \b 代表单词的开头或结尾。这样 \bfea\b 绝对不会匹配到 fear
        pattern = r'\b' + re.escape(word) + r'\b'
        match = re.search(pattern, lower_text)

        if match:
            best_match_index = match.start()
            matched_word_len = len(word)
            actual_word_used = word
            break

    if best_match_index == -1:
        logging.error("No context available.")
        return None

    # 1. 计算理论上的截取起止位置 (Ideal boundaries)
    ideal_start = max(0, best_match_index - window)
    ideal_end = min(len(raw_text), best_match_index + matched_word_len + window)

    # 2. 【智能边界对齐】：向外扩展，寻找最近的空格，防止单词被拦腰截断
    if ideal_start > 0: # 说明上下文不从文档开头开始
        # 在0到ideal_start之间，从右向左找最后一个空格
        space_idx = raw_text.rfind(' ', 0, ideal_start)
        if space_idx != -1:
            start = space_idx + 1  # 截取位置放在空格之后，避免断开单词
        else:
            start = 0  # 如果没找到空格，说明是一个超长词或者已经到开头了
    else:
        start = 0

    if ideal_end < len(raw_text):
        # 从ideal_end之后从左向右找第一个空格
        space_idx = raw_text.find(' ', ideal_end)
        if space_idx != -1:
            end = space_idx  # 截取位置放在空格处
        else:
            end = len(raw_text)
    else:
        end = len(raw_text)

    # 高亮显示匹配词
    snippet = raw_text[start:end].replace('\n', ' ')
    highlight_pattern = re.compile(r'\b(' + re.escape(actual_word_used) + r')\b', re.IGNORECASE)
    highlighted_snippet = highlight_pattern.sub(r'【\1】', snippet)

    # 如果已经顶到了文章开头/结尾，就不加省略号
    prefix = "..." if start > 0 else ""
    suffix = "..." if end < len(raw_text) else ""


    return f"{prefix}{highlighted_snippet.strip()}{suffix}"

if __name__ == "__main__":
    data_dir = 'dataset'
    doc_name = 'test.txt'
    res = generate_snippet(doc_name, query_words=['neighbor'], data_dir= 'dataset')
    print(res)
