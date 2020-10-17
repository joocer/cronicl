"""
IO

End-Points (sources and sinks) for streaming data processing.
"""

try:
    import ujson as json
except ImportError:
    import json
import csv
from pathlib import Path


def to_csv(dataset, filename, columns=['first_row']):
    """
    Saves a dataset as a CSV
    """
    import csv

    with open (filename, 'w', encoding='utf8', newline='') as file:
        
        # get the first record
        row = dataset.__next__()

        # get the columns from the record
        if columns==['first_row']:
            columns=record.keys()
        
        # write the headers
        csv_file = csv.DictWriter(file, fieldnames=columns)
        csv_file.writeheader()

        # cycle the rest of the file
        while row:
            row = _cronicl.select_fields(row, columns)
            csv_file.writerow(row)
            row = json_list.__next__()


def to_jsonl(dataset, filename):
    pass


def to_pandas(dataset):
    pass


def read_jsonl(filename, limit=-1, chunk_size=1024*1024, delimiter='\n'):
    """
    """
    file_reader = read_file(filename, chunk_size=chunk_size, delimiter=delimiter)
    line = next(file_reader)
    while line:
        yield(json.loads(line))
        limit -= 1
        if limit == 0:
            return
        try:
            line = next(file_reader)
        except:
            return

def read_csv(filename):
    pass


def read_file(filename, chunk_size=1024*1024, delimiter='\n'):
    """
    Reads an arbitrarily long file, line by line
    """
    with open(filename, 'r', encoding="utf8") as f:
        carry_forward = ''
        chunk = 'INITIALIZED'
        while len(chunk) > 0:
            chunk = carry_forward + f.read(chunk_size)
            lines = chunk.split(delimiter)
            carry_forward = lines.pop()
            yield from lines
        if carry_forward:
            yield carry_forward


def read_csv_lines(filename):
    with open(filename, "r", encoding='utf-8') as csvfile:
        datareader = csv.reader(csvfile)
        headers = next(datareader)
        row = next(datareader)
        while row:
            yield dict(zip(headers, row))
            try:
                row = next(datareader)
            except:
                row = next(datareader)


def write_jsonl(filename, data):
    with open(filename, "w", encoding='utf-8') as jsonfile:
        for r in data:
            #print(r)
            #time.sleep(0.5)
            try:
                jsonfile.write(json.dumps(r) + '\n')
            except:
                print(r)


def generator_chunker(gen, chunk_size):
    idx = 0
    chunk = [None] * chunk_size
    item = next(gen)
    while item:
        chunk[idx] = item
        idx += 1
        if idx == chunk_size:
            yield chunk
            idx = 0
        try:
            item = next(gen)
        except:
            item = None
    yield chunk[0:idx]

def clear_screen():
    print(chr(27)+'[2j')
    print('\033c')
    print('\x1bc')