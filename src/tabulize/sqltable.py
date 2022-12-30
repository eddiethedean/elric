from typing import Any, Dict, Generator, Iterable, Mapping, Optional, Sequence, Protocol
from dataclasses import dataclass

import sqlalchemy as sa
import sqlalchemy.orm.session as sa_session
import sqlalchemize as sz
import alterize as alt

Engine = sa.engine.Engine


class iTable(Protocol):
    def iterrows(self) -> Generator[tuple, None, None]:
        ...

    @property
    def columns(self) -> Iterable[str]:
        ...

    def __getitem__(self, key) -> Iterable:
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

    def set_primary_keys(self, column_names: Iterable[str]) -> None:
        for name in column_names:
            ...
            #alt.replace_primary_key()
            #alt.create_primary_key()

    def pk_tuples(self, records: list[dict]) -> set[tuple]:
        pks = self.primary_keys
        return set(tuple(record[key] for key in pks) for record in records)

    def old_pk_tuples(self) -> set[tuple]:
        return self.pk_tuples(self.old_records)

    def pk_record_from_tuple(self, pk_tuple: tuple) -> dict:
        pks = self.primary_keys
        return {key: val for key, val in zip(pks, pk_tuple)}

    def pk_record_from_record(self, record: dict) -> dict:
        pks = self.primary_keys
        return {key: record[key] for key in pks}

    def pk_records_from_tuples(self, pk_tuples) -> list[dict]:
        return [self.pk_record_from_tuple(t) for t in pk_tuples]

    def pk_records_from_records(self, records: list[dict]) -> list[dict]:
        return [self.pk_record_from_record(record) for record in records]

    def new_records(self, table: iTable) -> list[dict]:
        """Check for new records in data"""
        current_records = table_records(table)
        if len(current_records) == 0:
            return []
        old_pk_tuples = self.old_pk_tuples()
        current_pk_tuples = self.pk_tuples(current_records)
        new_pk_tuples = current_pk_tuples - old_pk_tuples
        if len(new_pk_tuples) == 0:
            return []
        new_pk_records = self.pk_records_from_tuples(new_pk_tuples)
        return self.full_current_records_from_pk_records(new_pk_records, table)

    def full_current_records_from_pk_records(self, pk_records: list[dict], table: iTable) -> list[dict]:
        full_current_records = []
        for current_record in table_records(table):
            for pk_record in pk_records:
                if self.pk_record_from_record(current_record) == pk_record:
                    full_current_records.append(current_record)
        return full_current_records

    def missing_records(self, table: iTable) -> list[dict]:
        """
        Check for missing records in data
        Return missing records primary key values.
        """
        current_records = table_records(table)
        old_pk_tuples = self.old_pk_tuples()
        current_pk_tuples = self.pk_tuples(current_records)
        missing_pk_tuples = old_pk_tuples - current_pk_tuples
        if len(missing_pk_tuples) == 0:
            return []
        return self.pk_records_from_tuples(missing_pk_tuples)

    def matching_pk_records(self, table: iTable) -> list[dict]:
        current_records = table_records(table)
        if len(current_records) == 0:
            return []
        old_pk_tuples = self.old_pk_tuples()
        current_pk_tuples = self.pk_tuples(current_records)
        matching_pk_tuples = old_pk_tuples & current_pk_tuples
        if len(matching_pk_tuples) == 0:
            return []
        matching_pk_records = self.pk_records_from_tuples(matching_pk_tuples)
        return self.full_current_records_from_pk_records(matching_pk_records, table)

    def delete_records(self, records: list[dict]) -> None:
        sa_table = sz.features.get_table(self.name, self.engine)
        delete_records_by_values(sa_table, self.engine, records)

    def insert_records(self, records: list[dict]) -> None:
        sa_table = sz.features.get_table(self.name, self.engine)
        sz.insert.insert_records(sa_table, records, self.engine)

    def update_records(self, records: list[dict]) -> None:
        sa_table = sz.features.get_table(self.name, self.engine)
        sz.update.update_records(sa_table, records, self.engine)
        
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
            
        # TODO: Check if primary keys are the same
        if self.primary_keys_different():
            # no: change primary keys
            ...
            
        new_records = self.new_records(table)
        if new_records:
            self.insert_records(new_records)
            
        missing_records = self.missing_records(table)
        if missing_records:
            self.delete_records(missing_records)
            
        matching_pk_records = self.matching_pk_records(table)
        if matching_pk_records:
            self.update_records(matching_pk_records)


def table_records(table: iTable) -> list[dict]:
    return [dict(row) for _, row in table.iterrows()]


def delete_records_by_values(
    sa_table: sa.Table,
    engine: Engine,
    records: list[dict]
) -> None:
    session = sa_session.Session(engine)
    try:
        sz.delete.delete_records_by_values_session(sa_table, records, session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

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



