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