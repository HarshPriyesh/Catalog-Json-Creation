from datatype_convert import *
from check_unwanted_character import *


def convert_schema_to_json(name, json_file, text_file, destination, source):
    f = open(source + '/' + text_file, 'r')
    lines = f.read().split('\n')
    f.close()

    check_unwanted_character(lines)

    print("INFO  :  Creating Catalog Json...")
    file1 = open(destination + '/' + json_file, 'w')
    li = ["{\n", '  "name": "', name, '",\n', '  "description": "",\n', '  "displayName": "', name, '",\n',
          '  "linked_resource": "",\n',
          '  "schema": {"columns": [']
    file1.writelines(li)

    list_len = len(lines)
    for line in lines:
        current_idx = lines.index(line)
        list_end = list_len - current_idx

        parts = line.split()

        a = datatype_convert(parts[1].split('(', 1)[0].lower())

        if current_idx == 0:
            o = ('{"column": "', parts[0], '", "description": "null", "mode": "NULLABLE",', ' "type": "', a,
                 '"},\n')
        elif list_end != 1:
            o = (
                '    {"column": "', parts[0], '", "description": "null", "mode": "NULLABLE",', ' "type": "', a,
                '"},\n')
        else:
            o = (
                '    {"column": "', parts[0], '", "description": "null", "mode": "NULLABLE",', ' "type": "', a,
                '"}\n')
        file1.writelines(o)

    m = ["  ]}\n", "}"]

    file1.writelines(m)
    print(f'INFO  :  Catalog Json Created- "{json_file}"')
    file1.close()
