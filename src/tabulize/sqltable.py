from typing import Any, Dict, Generator, Iterable, Mapping, Optional, Sequence, Protocol
from dataclasses import dataclass

import sqlalchemy as sa
import sqlalchemy.orm.session as sa_session
import sqlalchemize as sz
import alterize as alt

from tabulize.records_changes import records_changes

Engine = sa.engine.Engine
Record = dict[str, Any]


class iTable(Protocol):
    def iterrows(self) -> Generator[tuple[int, Record], None, None]:
        ...

    @property
    def columns(self) -> Sequence[str]:
        ...

    def __getitem__(self, key) -> Sequence:
        ...


class SqlTable:
    def __init__(self, name: str, engine: Engine):
        self.name = name
        self.engine = engine
        self.pull()

    def __repr__(self) -> str:
        return f'SqlTable(name={self.name}, columns={self.old_column_names}, dtypes={self.old_column_types})'
        
    def pull(self) -> None:
        """Pull table data from sql database"""
        sqltable = sz.features.get_table(self.name, self.engine)
        records = sz.select.select_records_all(sqltable, self.engine)
        self.old_records = list(records)
        self.old_column_names = sz.features.get_column_names(sqltable)
        self.old_column_types = sz.features.get_column_types(sqltable)
        self.old_primary_keys = sz.features.primary_key_names(sqltable)
        self.primary_keys = list(self.old_primary_keys)
        self.old_name = self.name

    def name_changed(self) -> bool:
        return self.old_name != self.name

    def change_name(self) -> None:
        """Change the name of the sql table to match current name."""
        alt.rename_table(self.old_name, self.name, self.engine)

    def missing_columns(self, table: iTable) -> set[str]:
        """Check for missing columns in data that are in sql table"""
        return set(self.old_column_names) - set(table.columns)

    def delete_columns(self, columns: Iterable[str]) -> None:
        for col_name in columns:
            alt.drop_column(self.name, col_name, self.engine)

    def extra_columns(self, table: iTable) -> set[str]:
        """Check for extra columns in data that are not in sql table"""
        return set(table.columns) - set(self.old_column_names)

    def create_column(self, column_name: str, table: iTable) -> None:
        """Create columns in sql table that are in data"""
        dtype = str
        for python_type in sz.type_convert._type_convert:
            if all(type(val) == python_type for val in table[column_name]):
                dtype = python_type
        alt.add_column(self.name, column_name, dtype, self.engine)

    def create_columns(self, column_names: Iterable[str], table: iTable) -> None:
        """Create a column in sql table that is in data"""
        for col_name in column_names:
            self.create_column(col_name, table)

    def primary_keys_different(self) -> bool:
        return set(self.old_primary_keys) != set(self.primary_keys)

    def set_primary_keys(self, column_names: list[str]) -> None:
        sqltable = sz.features.get_table(self.name, self.engine)
        alt.replace_primary_keys(sqltable, column_names, self.engine)

    def delete_records(self, records: list[dict]) -> None:
        sa_table = sz.features.get_table(self.name, self.engine)
        sz.delete.delete_records_by_values(sa_table, self.engine, records)

    def insert_records(self, records: list[dict]) -> None:
        sa_table = sz.features.get_table(self.name, self.engine)
        sz.insert.insert_records(sa_table, records, self.engine)

    def update_records(self, records: list[dict]) -> None:
        sa_table = sz.features.get_table(self.name, self.engine)
        sz.update.update_records_fast(sa_table, records, self.engine)

    def record_changes(self, table: iTable) -> dict[str, list[Record]]:
        return records_changes(self.old_records, table_records(table), self.primary_keys)
        
    def push(self, table: iTable) -> None:
        """
        Push any data changes to sql database table
        """
        if self.name_changed():
            self.change_name()
                
        missing_columns = self.missing_columns(table)
        if missing_columns:
            self.delete_columns(missing_columns)

        extra_columns = self.extra_columns(table)
        if extra_columns:
            self.create_columns(extra_columns, table)
            
        # TODO: Check if data types match
            # no: change data types of columns
            
        if self.primary_keys_different():
            self.set_primary_keys(self.primary_keys)
        
        changes = self.record_changes(table)
        new_records = changes['insert']
        if new_records:
            self.insert_records(new_records)
            
        missing_records = changes['delete']
        if missing_records:
            self.delete_records(missing_records)
          
        changed_records = changes['update']
        if changed_records:
            self.update_records(changed_records)


def table_records(table: iTable) -> list[dict]:
    return [dict(row) for _, row in table.iterrows()]


"""
def read_sql_data(
    table_name: str,
    engine: Engine,
    schema: Optional[str] = None
) -> Dict[str, list]:
    table = sz.features.get_table(table_name, engine, schema)
    records = sz.select.select_records_all(table, engine)
    return ttrows.row_dicts_to_data(records)


def read_sql_table(
    table_name: str,
    engine: Engine,
    schema: Optional[str] = None
) -> SqlTable:
    return SqlTable(table_name, engine)"""



