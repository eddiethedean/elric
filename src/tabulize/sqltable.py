from typing import Any, Dict, Mapping, Optional, Sequence

import tinytim.data as ttdata
import tinytim.rows as ttrows
from tinytable import Table

import sqlalchemy as sa
import sqlalchemize as sz
import alterize as alt
import tinytim as tt

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
        if self.old_name != self.name:
            # yes: change name of table
            alt.rename_table(self.old_name, self.name, self.engine)
                
        # Check for missing column names
        missing_cols = set(self.old_column_names) - set(self.columns)
        if missing_cols:
            # yes: delete columns from sql table
            for col_name in missing_cols:
                alt.drop_column(self.name, col_name, self.engine)

        # Check for extra column names
        extra_cols = set(self.columns) - set(self.old_column_names)
        if extra_cols:
            # yes: create new columns
            for col_name in extra_cols:
                # get column type
                dtype = sz.type_convert.get_sql_type(self.data[col_name])
                # create columns
                alt.add_column(self.name, col_name, dtype, self.engine)
                
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



