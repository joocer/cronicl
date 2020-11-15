import warnings
import logging


"""
select_fields
select_rows
column_to_list
concatenate
set_column
set_field
filter_dataset
fill_none
replace_values
merge_datasets


Deduplicate (buffer_size)
Reorder (buffer_size)
Summarize -> min, max, mean, mode, median, std, iqr, variance
Copy (make another generator)
"""

"""
dataset = a data set (list of dictionaries)
row = a record within a dataset
field = a labelled piece of data in a row
column = a field across multiple rows
value = a piece of data
item = field/value pair
"""


INNER_JOIN = 1
LEFT_JOIN = 2


def _select_all(dummy):
    """
    Always returns True.
    """
    return True


def select_fields(dic, fields):
    """
    Selects items from a row, if the row doesn't exist, None is used.
    """
    return {field: dic.get(field, None) for field in fields}


def select_columns(dataset, columns):
    """"""
    for row in dataset:
        yield select_fields(row, columns)


def column_to_list(dataset, column):
    """"""
    for row in dataset:
        yield row.get(column)


def concatenate(datasets):
    """"""
    for dataset in datasets:
        for row in dataset:
            yield row


def set_column(dataset, column, setter):
    """"""
    for row in dataset:
        yield set_field(row, column, setter)


def set_field(row, column, setter):
    """"""
    if type(setter).__name__ == "function":
        row[column] = setter(row)
    else:
        row[column] = setter
    return row


def filter_dataset(dataset, columns=["*"], condition=_select_all):
    """"""
    for row in dataset:
        if condition(row):
            if columns != ["*"]:
                row = select_fields(row, columns)
            yield row


def fill_none(dataset, filler=""):
    """
    Replaces 'None' values in a dataset with a default
    """
    return replace_values(dataset, None, filler)


def replace_values(dataset, oldvalue, newvalue):
    """
    Replace all instances of a value.
    """
    for row in dataset:
        yield {
            field: (newvalue if value is oldvalue else value)
            for field, value in row.items()
        }


def merge_datasets(left, right, column, join_type=INNER_JOIN):

    right_index = {}
    for row in right:
        index_value = row[column]
        right_index[index_value] = row

    for row in left:
        value = row.get(column)
        if right_index.get(value):
            yield {**row, **right_index[value]}
        elif join_type == LEFT_JOIN:
            yield row
