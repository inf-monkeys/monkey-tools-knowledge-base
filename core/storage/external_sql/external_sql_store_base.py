from __future__ import annotations

from abc import ABC, abstractmethod

from core.models.table import Table


class BaseExternalSQLStore(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def get_tables(self, **kwargs) -> list[Table]:
        raise NotImplementedError

    @abstractmethod
    def list_table_records(self, table_name, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def execute_sql(self, sql: str):
        raise NotImplementedError
