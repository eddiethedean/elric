from typing import Sequence
from tinytable import Table
import tabulize
from tinytim.rows import row_dicts_to_data
from sqlalchemy.engine import Engine


class TinySqlTable(Table):
    def __init__(self, name: str, engine: Engine):
        self.sqltable = tabulize.SqlTable(name, engine)
        data = row_dicts_to_data(self.sqltable.old_records)
        super().__init__(data)

    @property
    def primary_keys(self) -> list[str]:
        return self.sqltable.primary_keys

    @primary_keys.setter
    def primary_keys(self, column_names: Sequence[str]) -> None:
        self.sqltable.primary_keys = list(column_names)

    def pull(self):
        self.sqltable.pull()

    def push(self) -> None:
        self.sqltable.push(self)
        self.pull()


    