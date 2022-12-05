from typing import Any, Dict, Mapping, Optional, Sequence

import tinytim.data as ttdata
import tinytim.rows as ttrows
from tinytable import Table

import sqlalchemy as sa
import sqlalchemize as az

Engine = sa.engine.Engine


class SqlTable(Table):
    def __init__(
        self,
        name: str,
        engine: Engine,
        primary_key: Optional[str] = None,
        data: Mapping[str, Sequence] = {},
        schema: Optional[str] = None
    ) -> None:
        self.name: str = name
        self.engine: Engine = engine
        self.schema: None | str = schema
        self.data: Dict[str, list]
        table_names = az.features.get_table_names(engine, schema)
        if name in table_names:
            self.pull()

        elif name not in table_names and primary_key is not None:
            self.data = {name: list(col) for name, col in data.items()}
            if primary_key in ttdata.column_names(self.data):
                self.primary_key = primary_key
            else:
                raise ValueError('primary_key must match a column name')
        else:
            raise ValueError('primary_key cannot be None when sql table does not exist yet.')

    def pull(self) -> None:
        self.data = read_sql_data(self.name, self.engine, self.schema)
    
    def push(self) -> None:
        data_to_sql_table(self.name, self.data, self.engine, self.schema)


def data_to_sql_table(name, data, engine, schema=None) -> None:
    ...

        

def read_sql_data(
    table_name: str,
    engine: Engine,
    schema: Optional[str] = None
) -> Dict[str, list]:
    table = az.features.get_table(table_name, engine, schema)
    records = az.select.select_records_all(table, engine)
    return ttrows.row_dicts_to_data(records)


def read_sql_table(
    table_name: str,
    engine: Engine,
    primary_key: Optional[str],
    schema: Optional[str] = None
) -> SqlTable:
    data = read_sql_data(table_name, engine, schema)
    return SqlTable(table_name, engine, primary_key, data, schema)



