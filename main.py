import os
import yaml


def read_yaml_file(file_path):
    with open(file_path, "r") as file:
        data = yaml.safe_load(file)
    return data


def datatype_convert(argument):
    switcher = {
        "char": "STRING",
        "varchar": "STRING",
        "binary": "BYTES",
        "tinyint": "INTEGER",
        "smallint": "INTEGER",
        "int": "INTEGER",
        "bigint": "INTEGER",
        "decimal": "FLOAT",
        "numeric": "FLOAT",
        "double": "FLOAT",
        "struct": "RECORD",
        "map": "RECORD",
    }
    return switcher.get(argument, argument.upper())


def remove_trailing_comma_from_json(file_path):
    with open(file_path, "r") as file:
        lines = file.readlines()
    wordsInLine = lines[-3].split("}")
    lines[-3] = wordsInLine[0] + "}" + "\n"
    with open(file_path, "w") as file:
        file.writelines(lines)


def check_datatype(line, datatypes):
    for datatype in datatypes:
        if datatype in line:
            return True
    return False





def shorten_name(DBtableName, max_length=63):
    global trim_name
    length = len(DBtableName)
    print(f'INFO  :  Number of characters in "{DBtableName}" - {length}')
    if length > max_length:
        print("ERROR :  Name contains more than 64 characters. Trimming the name...")
        diff = (len(DBtableName) - max_length) + 3
        DBtableName = DBtableName[diff:]
        trim_name += 1
        print(
            f"INFO  :  After trimming - {DBtableName} ({len(DBtableName)} characters)"
        )
    return DBtableName


def convert_schema_to_json(dirpath, file):
    print(f'INFO  :  "{file}" found at "{dirpath}"')
    hql = open(os.path.join(dirpath, file), "r")
    print(f'INFO  :  Reading..."{file}"')
    skip_mode = False
    jsonCount = 0
    for line in hql:
        wordsInLine = line.lstrip().rstrip().replace("`", "").split(" ")
        if "create " in line.lower() and " database " not in line.lower():
            temp1 = line.lstrip().rstrip().replace("`", "").split(" ")
            tableName = temp1[-1].split(".")[-1].replace("(", "").lower()
            DBtableName = database + "." + tableName
            print(f"-----\nConversion {jsonCount+1}")
            print(f"INFO  :  Table Name - {tableName}")
            DBtableName = shorten_name(DBtableName)
            catalogJson = open(os.path.join(destination, DBtableName + ".json"), "w")
            jsonCount += 1
            print(f'INFO  :  Creating Catalog Json, {DBtableName + ".json"}')
            l = [
                "{\n",
                '  "name": "',
                DBtableName,
                '",\n',
                '  "description": "",\n',
                '  "displayName": "',
                DBtableName,
                '",\n',
                '  "linked_resource": "",\n',
                '  "schema": {"columns": [\n',
            ]
            catalogJson.writelines(l)
        else:
            if "partitioned by" in line.lower():
                l1 = ["  ]}\n", "}"]
                catalogJson.writelines(l1)
                if ")" in line.lower():
                    continue
                skip_mode = True
            elif ")" in line and skip_mode:
                skip_mode = False
            elif not skip_mode:
                try:
                    if check_datatype(line, datatypes):
                        l2 = (
                            '    {"column": "',
                            wordsInLine[0],
                            '", "description": "null", "mode": "NULLABLE",',
                            ' "type": "',
                            datatype_convert(
                                wordsInLine[1].split("(", 1)[0].replace(",", "")
                            ),
                            '"},\n',
                        )
                        catalogJson.writelines(l2)
                except IndexError:
                    print(
                        f"ERROR :  List index out of range in table {tableName}:{wordsInLine}\n"
                    )
    catalogJson.close()
    hql.close()
    print(f"-----\nINFO  :  Total Catalog Json created: {jsonCount}")
    print(f"INFO  :  Number of table name trimmed: {trim_name}")


if __name__ == "__main__":
    source = r"./hql"
    trim_name = 0
    datatypes = [
        "char",
        "varchar",
        "binary",
        "tinyint",
        "smallint",
        "int",
        "bigint",
        "decimal",
        "numeric",
        "double",
        "struct",
        "map",
        "string",
        "bytes",
        "integer",
        "float",
        "record",
        "date",
        "timestamp",
        "boolean",
    ]

    yaml_file = "config.yaml"
    yaml_data = read_yaml_file(yaml_file)
    database = yaml_data.get("database")
    hqlFile = yaml_data.get("hql")
    destination = r"./CatalogJson/" + yaml_data.get("feedname")
    print("\nEXECUTION STARTED")
    print("*****")
    print(f'Feed_Name: {yaml_data.get("feedname")}')
    print(f"Database: {database}")
    print(f"HQL: {hqlFile}")
    print(f"Output_path: {destination}")
    print("*****")
    if not os.path.exists(destination):
        print(
            f'INFO  :  Destination directory path dose not exist, "{destination}"...creating directory'
        )
        os.makedirs(destination)
        print("INFO  :  Destination directory created")
    checkHQL = True
    for dirpath, dirnames, filenames in os.walk(source):
        for file in filenames:
            if file == hqlFile:
                checkHQL = False
                convert_schema_to_json(dirpath, file)
    if checkHQL:
        print("\nEXECUTION FAILED")
        print(f'ERROR :  "{hqlFile}" dose not exist\n')
        exit()
    for file in os.listdir(destination):
        remove_trailing_comma_from_json(destination + "/" + file)
    print("*****\nEXECUTION COMPLETE\n")
