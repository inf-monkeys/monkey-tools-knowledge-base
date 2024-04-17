from pydantic import BaseModel


class Table(BaseModel):
    name: str
    sql: str

    def serialize(self):
        return {
            "name": self.name,
            "sql": self.sql,
        }
