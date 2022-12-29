from typing import Any, Dict, Mapping, Optional, Sequence

import tinytim.data as ttdata
import tinytim.rows as ttrows
from tinytable import Table

import sqlalchemy as sa
import sqlalchemize as sz

Engine = sa.engine.Engine


class SqlTable(Table):
    def __init__(self, name: str, engine: sa.engine.Engine):
        self.name = name
        self.engine = engine
        self.pull()
        
    def pull(self) -> None:
        """Pull table data from sql database"""
        sqltable = sz.features.get_table(self.name, self.engine)
        records = sz.select.select_records_all(sqltable, self.engine)
        self.old_data = ttrows.row_dicts_to_data(records)
        self.old_column_names = sz.features.get_column_names(sqltable)
        self.old_column_types = sz.features.get_column_types(sqltable)
        self.old_primary_keys = sz.features.get_primary_key_constraints(sqltable)
        self.old_name = self.name
        if self.old_data == {}:
            self.old_data = {col: [] for col in self.old_column_names}
        super().__init__(self.old_data)
        
    def push(self) -> None:
        """
        Push any data changes to sql database table.
        """
        # Check if table name changed
            # yes: change name of table
                
        # Check for missing column names
            # yes: delete columns from sql table
                
        # Check for extra column names
            # yes: create new columns
                
        # Check if data types match
            # no: change data types of columns
            
        # Check if primary keys are the same
            # no: change primary keys
            
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



