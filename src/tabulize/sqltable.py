from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Protocol
from dataclasses import dataclass

import tinytim.data as ttdata
import tinytim.rows as ttrows
from tinytable import Table

import sqlalchemy as sa
import sqlalchemize as sz
import alterize as alt
import tinytim as tt

Engine = sa.engine.Engine

@dataclass
class iTable:
    data: Mapping[str, Sequence]
    columns: Sequence[str]



class SqlTable:
    def __init__(self, name: str, engine: sa.engine.Engine, table: iTable):
        self.name = name
        self.engine = engine
        self.table = table
        self.pull()
        
    def pull(self) -> None:
        """Pull table data from sql database"""
        sqltable = sz.features.get_table(self.name, self.engine)
        records = sz.select.select_records_all(sqltable, self.engine)
        self.old_data = ttrows.row_dicts_to_data(records)
        self.old_column_names = sz.features.get_column_names(sqltable)
        self.old_column_types = sz.features.get_column_types(sqltable)
        self.old_primary_keys = sz.features.primary_key_names(sqltable)
        self.primary_keys = list(self.old_primary_keys)
        self.old_name = self.name
        if self.old_data == {}:
            self.old_data = {col: [] for col in self.old_column_names}
        self.table.__init__(self.old_data, self.old_column_names)

    def name_changed(self) -> bool:
        return self.old_name != self.name

    def change_name(self) -> None:
        """Change the name of the sql table to match current name."""
        alt.rename_table(self.old_name, self.name, self.engine)

    def missing_columns(self) -> set[str]:
        """Check for missing columns in data that are in sql table"""
        return set(self.old_column_names) - set(self.table.columns)

    def delete_columns(self, columns: Iterable[str]) -> None:
        for col_name in columns:
            alt.drop_column(self.name, col_name, self.engine)

    def extra_columns(self) -> set[str]:
        """Check for extra columns in data that are not in sql table"""
        return set(self.columns) - set(self.old_column_names)

    def create_column(self, column_name: str) -> None:
        """Create columns in sql table that are in data"""
        dtype = sz.type_convert.get_sql_type(self.data[column_name])
        alt.add_column(self.name, column_name, dtype, self.engine)

    def create_columns(self, column_names: Iterable[str]) -> None:
        """Create a column in sql table that is in data"""
        for col_name in column_names:
            self.create_column(col_name)

    def primary_keys_different(self) -> bool:
        return set(self.old_primary_keys) != set(self.primary_keys)

    def set_primary_keys(self, column_names: Iterable[str]) -> None:
        for name in column_names:
            alt.replace_primary_key()
            alt.create_primary_key()
        
    def push(self) -> None:
        """
        Push any data changes to sql database table
        """
        if self.name_changed():
            self.change_name()
                
        missing_columns = self.missing_columns()
        if missing_columns:
            self.delete_columns(missing_columns)

        extra_columns = self.extra_columns()
        if extra_columns:
            self.create_columns(extra_columns)
            
        # Check if data types match
            # no: change data types of columns
            
        # Check if primary keys are the same
        if self.primary_keys_different():
            # no: change primary keys
            ...
            
        # Check for new primary key records
            # yes: insert new records
            
        # Check for missing primary key records
            # yes: delete records
            
        # Check each matching primary key record values match
            # no: update sql table records

        

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
    return SqlTable(table_name, engine)



