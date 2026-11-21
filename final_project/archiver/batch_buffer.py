from __future__ import annotations


class BatchBuffer:
    """Пачки сообщений по (топик, партиция): сброс по размеру или по возрасту пачки."""

    def __init__(self, max_messages: int = 500, max_age_seconds: float = 10):
        self.max_messages = max_messages
        self.max_age_seconds = max_age_seconds
        self._records: dict[tuple[str, int], list[dict]] = {}
        self._started: dict[tuple[str, int], float] = {}

    def add(self, topic: str, partition: int, record: dict, now: float) -> None:
        """Добавить сообщение; запомнить время начала пачки."""
        key = (topic, partition)   # «пачками по партициям»: у каждой партиции своя пачка
        self._records.setdefault(key, []).append(record)
        self._started.setdefault(key, now)

    def ready(self, now: float) -> list[tuple[str, int]]:
        """Пачки, которые пора записать."""
        return [key for key, records in self._records.items()
                if len(records) >= self.max_messages or now - self._started[key] >= self.max_age_seconds]

    def drain(self, topic: str, partition: int) -> list[dict]:
        """Забрать пачку из буфера."""
        self._started.pop((topic, partition), None)
        return self._records.pop((topic, partition), [])

    def drop(self, partitions: list[tuple[str, int]]) -> None:
        """Выбросить пачки партиций, которые отобрали при перебалансировке.

        Их offset не закоммичен, поэтому новый владелец партиции прочитает эти сообщения заново.
        """
        for topic, partition in partitions:
            self.drain(topic, partition)
