from typing import List, Optional, Union
import pandas as pd
from sqlalchemy.engine import Engine

"""
---Do basic sql opperations with Pandas DataFrames---
Create sql tables: to_sql_create
Insert DataFrame rows into existing sql table: to_sql_insert
Delete matching records in existing sql table: to_sql_delete
Update matching records in existing sql table: to_sql_update
Alter existing sql table to match Pandas DataFrame metadata: to_sql_alter
"""

def to_sql_create(
    df: pd.DataFrame,   
    name: str,
    engine: Engine,
    if_exists: str = 'fail',
    index: bool = False,
    dtype: Optional[dict] = None,
    primary_key: Union[str, List[str]] = [],
) -> None:
    """
    Create a new sql table from DataFrame features.
    """
    ...


def to_sql_insert(
    df: pd.DataFrame,   
    name: str,
    engine: Engine,
    index: bool = False,
    chunksize: Optional[int] = None,
) -> None:
    """
    Insert records from DataFrame into existing sql table.
    """
    ...