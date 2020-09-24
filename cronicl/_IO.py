"""
"""

import ._cronicl

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


def read_jsonl(filename):
    """
    """
    with open(filename, 'r') as file:
        line = file.readline()
        while line:
            yield json.loads(line)
            line = file.readline()


def read_csv(filename):
    pass




def read_file(filename, chunk_size=1024*1024, delimiter='\n'):
    """
    Reads an arbitrarily long file, line by line
        
    Returns an generator of lines in the file
    """
    
    file_size = file.size

    carry_forward = ''
    cursor = 0
    while (cursor < blob_size):
        chunk = file_read(start=cursor, end=min(file_size, cursor+chunk_size-1))   
        cursor = cursor + len(chunk)
        chunk = chunk.decode('utf-8')

        # add the last line from the previous cycle
        chunk = carry_forward + chunk
        carry_forward = ''
        lines = chunk.split(delimiter)
        if len(lines) == 1:
            yield chunk
        else:
            # the last line is likely to be incomplete, save it to carry forward
            carry_forward = lines[-1]  
            del lines[-1]
            for line in lines:
                yield line
    if len(carry_forward) > 0:
        yield carry_forward