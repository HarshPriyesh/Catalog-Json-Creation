import os

source = r"./hql"
destination = r"./jsonDatacatalog"
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
            jsonFileName = database + "." + tableName + ".json"
        else:
            if "partitioned by" in line.lower():
                if ")" in line.lower():
                    continue
                skip_mode = True
            elif ")" in line and skip_mode:
                skip_mode = False
            elif not skip_mode:
                if check_datatype(line, datatypes):
                    print(
                        wordsInLine[0], wordsInLine[1].split("(", 1)[0].replace(",", "")
                    )
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
