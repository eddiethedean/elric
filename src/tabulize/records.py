from typing import Any, Iterable, Sequence

Record = dict[str, Any]


def records_changes(
    records_old: Iterable[Record],
    records_new: Iterable[Record],
    key_columns: Sequence[str]
) -> dict[str, list[Record]]:
    """
    Find changes between original records (records_old)
    and new version of records (records_new).

    Both records must all have key_columns to work.
    
    Return dict of {'insert': list[new records],
                    'delete': list[deleted records],
                    'update': list[changed records]}

    Examples
    --------
    >>> records1 = [{'id': 5, 'x': 11, 'y': 55},
        {'id': 6, 'x': 11, 'y': 55},
        {'id': 7, 'x': 22, 'y': 6},
        {'id': 8, 'x': 33, 'y': 7},
        {'id': 9, 'x': 44, 'y': 4},
        {'id': 10, 'x': 55, 'y': 21}
    ]

    >>> records2 = [
        {'id': 7, 'x': 22, 'y': 6},
        {'id': 8, 'x': 55, 'y': 7},
        {'id': 9, 'x': 44, 'y': 6},
        {'id': 10, 'x': 55, 'y': 21},
        {'id': 11, 'x': 99, 'y': 90}
    ]
    >>> records_changes(records1, records2, ['id'])
    {'insert': [{'id': 11, 'x': 99, 'y': 90}],
     'update': [{'id': 8, 'x': 55, 'y': 7}, {'id': 9, 'x': 44, 'y': 6}],
     'delete': [{'id': 6, 'x': 11, 'y': 55}, {'id': 5, 'x': 11, 'y': 55}]}
    """
    changes = {'insert': [], 'update': []}
    dict_old = records_to_dict(records_old, key_columns)
    dict_new = records_to_dict(records_new, key_columns)
    for key_values_new, record_new in dict_new.items():
        if key_values_new in dict_old:
            if dict_old[key_values_new] != record_new:
                changes['update'].append(record_new)
        else:
            changes['insert'].append(record_new)
    missing_keys = set(dict_old.keys())  - set(dict_new.keys())
    changes['delete'] = [dict_old[key] for key in missing_keys]
    return changes


def records_to_dict(
    records: Iterable[Record],
    key_columns: Sequence[str]
) -> dict[tuple, Record]:
    """
    Organizes a list of dict records into
    a dict of {tuple[key values]: record}
    """
    return {
        tuple(val for key, val in record.items() if key in key_columns): record
            for record in records
        }


def filter_record(
    record: dict,
    key_columns: list[str]
) -> dict:
    return {key: record[key] for key in key_columns}


def matching_records(
    record1: dict,
    record2: dict,
    key_columns: list[str]
) -> bool:
    f1 = filter_record(record1, key_columns)
    f2 = filter_record(record2, key_columns)
    return f1 == f2

from collections import namedtuple
from typing import Iterable

MatchStatus = namedtuple('MatchStatus', ['status', 'record', 'index'])


def find_record(
    record1: dict,
    records: Sequence[dict],
    key_columns: list[str],
    not_found_indexes: set[int]
) -> MatchStatus:
    for i in not_found_indexes:
        record2 = records[i]
        if matching_records(record1, record2, key_columns):
            if record1 != record2:
                return MatchStatus('Update', record2, i)
            else:
                return MatchStatus('Same', record1, i)
    return MatchStatus('Missing', record1, -1)


def find_record_changes_slow(
    records1: Sequence[dict],
    records2: Sequence[dict],
    key_columns: list[str]
) -> dict[str, list[dict]]:
    """
    Loop through both iterables of records.
    Find inserts, updates, and deleted records.
    For when both tables can't fit in memory.

    For inserts, note all unmatched records in new data.
    For updates, note all matched records with changes.
    For deletes, note all unmatched records in old data.
    """
    not_found_indexes = set(range(len(records2)))
    missing_indexes: set[int] = set() 
    updated_records: list[dict] = []
    for index1, record1 in enumerate(records1):
        match = find_record(record1, records2, ['id'], not_found_indexes)
        if match.status == 'Missing':
            missing_indexes.add(index1) 
        else:
            not_found_indexes.remove(match.index)
            if match.status == 'Update':
                updated_records.append(match.record)
    inserted_records = [records2[i] for i in not_found_indexes]
    deleted_records = [records1[i] for i in missing_indexes]
    return {'insert': inserted_records,
            'update': updated_records,
            'delete': deleted_records}
        
if __name__ == '__main__':
    records1 = [{'id': 5, 'x': 11, 'y': 55},
        {'id': 6, 'x': 11, 'y': 55},
        {'id': 7, 'x': 22, 'y': 6},
        {'id': 8, 'x': 33, 'y': 7},
        {'id': 9, 'x': 44, 'y': 4},
        {'id': 10, 'x': 55, 'y': 21}
    ]
            
    records2 = [
            {'id': 7, 'x': 22, 'y': 6},
            {'id': 8, 'x': 55, 'y': 7},
            {'id': 9, 'x': 44, 'y': 6},
            {'id': 10, 'x': 55, 'y': 21},
            {'id': 11, 'x': 99, 'y': 90}
    ]
    not_found_indexes = set(range(len(records2)))
    missing_indexes: set[int] = set() 
    updated_records: list[dict] = []
    for index1, record1 in enumerate(records1):
        results = find_record(record1, records2, ['id'], not_found_indexes)
        if results == ('Missing', -1):
            missing_indexes.add(index1) 
        else:
            not_found_indexes.remove(results[1])
            if type(results[0]) is dict:
                updated_records.append(results[0])
                
    inserted_records = [records2[i] for i in not_found_indexes]
    deleted_records = [records1[i] for i in missing_indexes]
                
    print({'insert': inserted_records,
    'update': updated_records,
    'delete': deleted_records})