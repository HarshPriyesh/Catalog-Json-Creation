import os

source = r"./hql"
destination = r"./CatalogJson"
database = "consumption_rf_3_insfin_verona_mpf"
hqlFile = "example.hql"
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


for dirpath, dirnames, filenames in os.walk(source):
    # print(f"Current directory: {dirpath}")
    # print(f"Subdirectories: {dirnames}")
    # print(f"Files: {filenames}")
    # print("---")
    for file in filenames:
        if file == hqlFile:
            convert_schema_to_json(dirpath, file)
        else:
            pass
