import os
import yaml
import logging
import sys


def setup_logging(log_file):
    logging.basicConfig(level=logging.DEBUG)
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(format=log_format, level=logging.DEBUG)

    file_handler = logging.FileHandler(log_file)
    file_formatter = logging.Formatter(log_format)
    file_handler.setFormatter(file_formatter)
    logging.getLogger().addHandler(file_handler)


def redirect_stdout_to_file(log_file):
    sys.stdout = open(log_file, "w")


def restore_stdout(stdout_original):
    sys.stdout.close()
    sys.stdout = stdout_original


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
        if datatype in line.lower():
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


def skip_lines_until_create(lines):
    hql_skip_mode = False
    global rawDB
    if rawDB == "":
        rawDB = "raw_"
    for line in lines:
        if "create " in line.lower() and "database" in line.lower():
            hql_skip_mode = True
            continue
        if "create " in line.lower() and rawDB.lower() in line.lower():
            hql_skip_mode = True
            continue
        if "drop table " in line.lower():
            hql_skip_mode = True
            continue
        if "recon_table" in line.lower():
            hql_skip_mode = True
            continue
        if "create " in line.lower():
            hql_skip_mode = False
        if not hql_skip_mode:
            yield line


def convert_schema_to_json(dirpath, file):
    print(f'INFO  :  "{file}" found at "{dirpath}"')
    original_hql = open(os.path.join(dirpath, file), "r")
    print(f'INFO  :  Reading..."{file}"')
    filtered_hql = skip_lines_until_create(original_hql.readlines())
    print(f'INFO  :  Filtering HQL by keeping only schema table structure(s)"')
    original_hql.close()
    print(f'INFO  :  Creating temporary HQL, "temp.hql", with filtered HQL details')
    with open("temp.hql", "w") as output_file:
        output_file.writelines(filtered_hql)
    print(f'INFO  :  "temp.hql" created')
    temp_hql = open("temp.hql", "r")
    skip_mode = False
    jsonCount = 0
    entryNames = []
    locations = []
    for line in temp_hql:
        wordsInLine = line.lstrip().rstrip().replace("`", "").split(" ")
        if "create " in line.lower():
            temp1 = line.lstrip().rstrip().replace("`", "").split(" ")
            tableName = temp1[-1].split(".")[-1].replace("(", "").lower()
            DBtableName = database.lower() + "." + tableName
            print(f"-----\nConversion {jsonCount+1}")
            print(f"INFO  :  Table Name - {tableName}")
            DBtableName = shorten_name(DBtableName)
            entryNames.append(DBtableName)
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
        elif "location " in line.lower():
            if "'gs://" in line.lower():
                locations.append(
                    line.replace("LOCATION", "")
                    .replace(r"${hivevar:stagingBucket}", f"{bucket_name}")
                    .replace("'", "")
                    .replace(";", "")
                    .replace("\n", "")
                    .lstrip()
                )
            else:
                locations.append(
                    f"gs://{bucket_name}/{feedname}_{trouxid}/{trouxid}/{tableName}/"
                )
        else:
            if "partitioned by" in line.lower():
                l1 = ["  ]}\n", "}"]
                catalogJson.writelines(l1)
                if ")" in line:
                    continue
                skip_mode = True
            elif "location" in line.lower():
                if ";" in line:
                    continue
                skip_mode = True
            elif ")" in line and skip_mode:
                skip_mode = False
            elif ";" in line and skip_mode:
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
                                wordsInLine[1]
                                .split("(", 1)[0]
                                .replace(",", "")
                                .replace(")", "")
                            ),
                            '"},\n',
                        )
                        catalogJson.writelines(l2)
                except IndexError:
                    print(
                        f"ERROR :  List index out of range in table {tableName}:{wordsInLine}\n"
                    )
    catalogJson.close()
    temp_hql.close()
    try:
        os.remove("temp.hql")
        print(f'-----\nINFO  :  "temp.hql" has been deleted successfully.')
    except Exception as e:
        print(f"ERROR :  An error occurred while deleting the file: {e}")
    print(f"INFO  :  Total Catalog Json created: {jsonCount}")
    print(f"INFO  :  Number of table names trimmed: {trim_name}")
    if config_2:
        config2(entryNames, locations)


def config1(destination, config_file):
    print(f'INFO  :  GCP_AtlasJson_path - "{GCP_AtlasJson_path}"')
    print(f'INFO  :  GCP_CatalogJson_path - "{GCP_CatalogJson_path}"')
    print("INFO  :  Creating/Updating...data_catalog_create_schema_json_config.txt")
    count = 0
    entryNames = []
    locations = []
    with open(
        config_file + "/data_catalog_create_schema_json_config.txt", "w"
    ) as config1:
        config1.write("atlas_ectract,dest_storage_path\n")
        for filename in os.listdir(destination):
            if os.path.isfile(os.path.join(destination, filename)):
                entryNames.append(filename.replace(".json", ""))
                locations.append(
                    f"gs://{bucket_name}/{feedname}_{trouxid}/{trouxid}/{filename.split('.')[1]}/"
                )
                c1 = (
                    GCP_AtlasJson_path
                    + "/"
                    + filename
                    + ","
                    + GCP_CatalogJson_path
                    + "/"
                    + filename
                    + "\n"
                )
                config1.write(c1)
                count += 1
    print(f"INFO  :  Total lines updated in config1 file: {count}")
    if config_2:
        config2(entryNames, locations)


def config2(entryNames, locations):
    print("-----\nCONFIG 2 execution")
    print(f'INFO  :  entry_group - "{entry_group}"')
    print(f'INFO  :  bucket_name - "{bucket_name}"')
    print(
        f'INFO  :  Config files path: "{config_file}/data_catalog_create_ext_entries_config.txt"'
    )
    print("INFO  :  Creating/Updating...data_catalog_create_ext_entries_config.txt")
    count = 0
    if len(locations) == 0:
        for name in entryNames:
            locations.append(f"gs://{bucket_name}/{feedname}_{trouxid}/{trouxid}/{name.split('.')[-1]}/")
    with open(
        config_file + "/data_catalog_create_ext_entries_config.txt", "w"
    ) as config2:
        config2.write("entry_group,entry,schema_path,linked_resources\n")
        for name, location in zip(entryNames, locations):
            c2 = (
                entry_group
                + ","
                + name
                + ","
                + GCP_CatalogJson_path
                + ","
                + f"{location}\n"
            )
            config2.write(c2)
            count += 1
    print(f"INFO  :  Total lines updated in config2 file: {count}")


if __name__ == "__main__":
    log_file = "execution.log"
    setup_logging(log_file)
    stdout_original = sys.stdout
    redirect_stdout_to_file(log_file)

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
    yaml_file = "parameter.yaml"
    yaml_data = read_yaml_file(yaml_file)
    feedname = yaml_data.get("feedname")
    trouxid = yaml_data.get("trouxid")
    database = yaml_data.get("schema_database")
    rawDB = yaml_data.get("raw_database")
    hqlFile = yaml_data.get("hql")
    config_1 = bool(yaml_data.get("config1").lower() == "true")
    config_2 = bool(yaml_data.get("config2").lower() == "true")
    entry_group = yaml_data.get("entry_group")
    bucket_name = yaml_data.get("bucket_name")
    GCP_CatalogJson_path = yaml_data.get("GCP_CatalogJson_path")
    GCP_AtlasJson_path = yaml_data.get("GCP_AtlasJson_path")

    print("\nEXECUTION STARTED")
    print("*******")
    print(f"Feed Name: {feedname}")
    print(f"TrouxID: {trouxid}")
    print(f"Schema Database: {database}")
    print(f"Raw Database: {rawDB}")
    print(f"HQL: {hqlFile}")
    if config_1:
        destination = yaml_data.get("atlasJson_path")
        print(f'Atlas_Json_Path: "{destination}"')
    else:
        destination = r"./CatalogJson/" + feedname
        print(f"Catalog_Json_Path: {destination}")
    print("*******")
    if not os.path.exists(destination):
        print(f'INFO  :  Path dose not exist, "{destination}"')
        if config_1:
            print("ERROR :  Mention the correct Atlas Json path to proceed")
            print("*******\nEXECUTION FAILED")
            restore_stdout(stdout_original)
            exit()
        print(f"INFO  :  Creating directory...")
        os.makedirs(destination)
        print(f'INFO  :  Path directory created, "{destination}"')
    config_file = r"./Configs/" + feedname
    if config_1 or config_2:
        if not os.path.exists(config_file):
            print(f'INFO  :  Config path dose not exist, "{config_file}"')
            print(f"INFO  :  Creating directory...")
            os.makedirs(config_file)
            print(f'INFO  :  Config directory created, "{config_file}"')
    if config_1:
        print("CONFIG 1 execution")
        if len(os.listdir(destination)) == 0:
            print(f"ERROR :  The directory is empty, {destination}")
            print("*******\nEXECUTION FAILED")
            restore_stdout(stdout_original)
            exit()
        print(f'INFO  :  Config files path: "{config_file}/data_catalog_create_schema_json_config.txt"')
        config1(destination, config_file)
    else:
        checkHQL = True
        for dirpath, dirnames, filenames in os.walk(source):
            for file in filenames:
                if file == hqlFile:
                    checkHQL = False
                    convert_schema_to_json(dirpath, file)
        if checkHQL:
            print(f'ERROR :  "{hqlFile}" dose not exist')
            print("EXECUTION FAILED\n")
            exit()
        for file in os.listdir(destination):
            remove_trailing_comma_from_json(destination + "/" + file)
    print("*******\nEXECUTION COMPLETED")

    restore_stdout(stdout_original)
