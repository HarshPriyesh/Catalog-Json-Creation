import os
from convert_schema_to_json import *
from status import *


current = os.getcwd()
print("\nSTARTED")
if os.path.exists(os.path.dirname(__file__) + "/../config.txt"):
    print(f'INFO :  Reading..."{current}\\config.txt"')
    config = open(os.path.dirname(__file__) + "/../config.txt", 'r')
    lines = config.readlines()
    config.close()

    count = 0
    run = 0

    for i in lines[1:]:
        source = i.split(',')[0].strip()
        file = i.split(',')[1].strip().lower()
        destination = i.split(',')[2].strip()

        flag = 0
        run = run + 1

        print(f'Executing line {run}')
        if not os.path.exists(source):
            print(f'ERROR :  Source directory path dose not exist- "{source}"')
            fail()
        elif not os.listdir(source):
            print(f'ERROR :  Source directory, "{source}", is EMPTY')
            fail()
        else:
            m = len([entry for entry in os.listdir(source) if os.path.isfile(os.path.join(source, entry))])
            print(f'INFO  :  Number of schema file present at Source directory path "{source}"- {m}')

        if not os.path.exists(destination):
            print(f'ERROR :  Destination directory path dose not exist, "{destination}"...creating directory')
            os.makedirs(destination)
            print("INFO  :  Destination directory created")

        print(f'INFO  :  Creating catalog json at "{destination}"')
        for text_file in os.listdir(source):
            if not text_file.endswith('.txt'):
                print(f'ERROR :  Schema file format is not ".txt"')
                fail()
            name = text_file.split('.txt', 1)[0].lower()
            if name == file:
                print(f'INFO  :  Schema File- "{text_file}"')
                flag = 1
                print(f'INFO  :  File name without extension- "{name}"')
                length = len(name)
                if length >= 64:
                    print(f'ERROR :  File name contains more than 64 characters- "{name}"')
                    print(f"ERROR :  File name length- {length}")
                    fail()
                else:
                    print(f"INFO  :  Number of characters in File Name- {length}")
                json_file = name + '.json'
                if os.path.exists(destination + '/' + json_file):
                    print(f'ERROR :  Catalog Json already exists- "{json_file}"')
                    break
                name = text_file.split('.txt', 1)[0]
                convert_schema_to_json(name, json_file, text_file, destination, source)
                count = count + 1
                break

        if flag == 0:
            print(f'ERROR :  Schema file does not exist- "{file}.txt"')
            fail()
    passed(count)
else:
    print(f'ERROR :  "{current}\\config.txt"...file dose not exist')
    fail()
