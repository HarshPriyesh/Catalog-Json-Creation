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


def convert_schema_to_json(dirpath, file):
    hql = open(os.path.join(dirpath, file), "r")
    skip_mode = False
    for line in hql:
        wordsInLine = line.lstrip().rstrip().replace("`", "").split(" ")
        if "create " in line.lower():
            temp1 = line.lstrip().rstrip().replace("`", "").split(" ")
            tableName = temp1[-1].split(".")[-1].replace("(", "").lower()
            DBtableName = database + "." + tableName
            catalogJson = open(os.path.join(destination, DBtableName + ".json"), "w")
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
    catalogJson.close()
    hql.close()


if __name__ == "__main__":
    source = r"./hql"
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
    ]

    yaml_file = "config.yaml"
    yaml_data = read_yaml_file(yaml_file)
    print("\nEXECUTION STARTED")
    destination = r"./CatalogJson/" + yaml_data.get("feedname")
    if not os.path.exists(destination):
        print(
            f'INFO  :  Destination directory path dose not exist, "{destination}"...creating directory'
        )
        os.makedirs(destination)
        print("INFO  :  Destination directory created")
    database = yaml_data.get("database")
    hqlFile = yaml_data.get("hql")
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
