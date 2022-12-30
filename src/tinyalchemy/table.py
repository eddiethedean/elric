from tinytable import Table
import tabulize
from tinytim.rows import row_dicts_to_data
from sqlalchemy.engine import Engine


class TinySqlTable(Table):
    def __init__(self, name: str, engine: Engine):
        self.sqltable = tabulize.SqlTable(name, engine)
        data = row_dicts_to_data(self.sqltable.old_records)
        super().__init__(data)

    def pull(self):
        self.sqltable.pull()

    def push(self) -> None:
        self.sqltable.push(self)
        self.pull()


    