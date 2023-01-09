from typing import Iterable


def records_changes(
    records_old: Iterable[dict],
    records_new: Iterable[dict],
    key_columns: list[str]
) -> dict[str, list]:
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
    records: Iterable[dict],
    key_columns: list[str]
) -> dict[tuple, dict]:
    """
    Organizes a list of dict records into
    a dict of {tuple[key values]: record}
    """
    return {
        tuple(val for key, val in record.items() if key in key_columns): record
            for record in records
        }


"""
Loop through chunks of both records.
Find inserts, updates, and deleted records.
For when both tables cant fit in memory.

For inserts, need to note all unmatched records in new data.
For updates, need to note all matched records with changes.
For deletes, need to note all unmatched records in old data.
"""
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


def find_record(
    record1: dict,
    records: list[dict],
    key_columns: list[str],
    not_found_indexes: set[int]
) -> tuple[dict | str, int]:
    for i in not_found_indexes:
        record2 = records[i]
        if matching_records(record1, record2, key_columns):
            if record1 != record2:
                return record2, i
            else:
                return 'Same', i
    return 'Missing', -1
        
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