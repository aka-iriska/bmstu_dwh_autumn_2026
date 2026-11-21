from __future__ import annotations

import re

# (?U) - \s понимает Unicode-пробелы в Java-регулярках Spark (в Python это поведение по умолчанию).
TOKEN_SPLIT_REGEX = r"(?U)\s+"
MIN_WORD_LEN = 5  # «длиннее четырёх символов»


# одни и те же правила для DataFrame и RDD (data/README.md)
def tokenize(query: str) -> list[str]:
    """Слова запроса по правилам задания (Python-версия для RDD)."""
    return [word for word in re.split(r"\s+", query.lower()) if len(word) >= MIN_WORD_LEN]
