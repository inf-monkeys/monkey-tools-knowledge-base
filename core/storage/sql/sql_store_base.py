from __future__ import annotations

from abc import ABC, abstractmethod

from core.models.table import Table


class BaseSQLStore(ABC):

    def __init__(self, database_name: str):
        self.database_name = database_name

    @abstractmethod
    def create_table(self, **kwargs) -> bool:
        raise NotImplementedError

    @abstractmethod
    def create_database(self, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def get_tables(self, **kwargs) -> list[Table]:
        raise NotImplementedError

    @abstractmethod
    def drop_database(self, **kwargs):
        raise NotImplementedError
