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
from ._datasets import select_fields


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
            columns=dataset.keys()
        
        # write the headers
        csv_file = csv.DictWriter(file, fieldnames=columns)
        csv_file.writeheader()

        # cycle the rest of the file
        while row:
            row = select_fields(row, columns)
            csv_file.writerow(row)
            row = dataset.__next__()


def read_jsonl(filename, limit=-1, chunk_size=1024*1024, delimiter='\n'):
    """
    """
    file_reader = read_file(filename, chunk_size=chunk_size, delimiter=delimiter)
    line = next(file_reader)
    while line:
        yield json.loads(line)
        limit -= 1
        if limit == 0:
            return
        try:
            line = next(file_reader)
        except StopIteration:
            return


def read_file(filename, chunk_size=1024*1024, delimiter='\n'):
    """
    Reads an arbitrarily long file, line by line
    """
    with open(filename, 'r', encoding="utf8") as f:
        carry_forward = ''
        chunk = 'INITIALIZED'
        while len(chunk) > 0:
            chunk = f.read(chunk_size)
            augmented_chunk = carry_forward + chunk
            lines = augmented_chunk.split(delimiter)
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
            except StopIteration:
                row = None


def write_jsonl(filename, data):
    with open(filename, "w", encoding='utf-8') as jsonfile:
        for r in data:
            try:
                jsonfile.write(json.dumps(r) + '\n')
            except ValueError:
                jsonfile.write("*****" + '\n')


#https://gist.github.com/nwjlyons/621fabfc0d4c1119b2ad338f615ce4ef#file-chunks-py
def generator_chunker(generator, chunk_size):
    """Yield successive chunks from a generator"""
    chunk = []

    for item in generator:
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = [item]
        else:
            chunk.append(item)

    if chunk:
        yield chunk
    

def clear_screen():
    print(chr(27)+'[2j')
    print('\033c')
    print('\x1bc')
