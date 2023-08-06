import os
import yaml
import logging
import sys
import pandas as pd


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
    global trim_name
    trim_name = 0
    jsonCount = 0
    entryNames = []
    locations = []
    for line in temp_hql:
        wordsInLine = line.lstrip().rstrip().replace("`", "").split(" ")
        if "create " in line.lower():
            temp1 = (
                line.lstrip()
                .rstrip()
                .replace("`", "")
                .replace(" (", "")
                .replace("(", "")
                .split(" ")
            )
            tableName = temp1[-1].split(".")[-1].lower()
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
        else:
            if "partitioned by" in line.lower():
                l1 = ["  ]}\n", "}"]
                catalogJson.writelines(l1)
                if ")" in line:
                    continue
                skip_mode = True
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
                if ";" in line:
                    continue
                skip_mode = True
            elif ")," in line and skip_mode:
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
                                .lower()
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
    if trim_name > 0:
        print(f"INFO  :  Number of table names trimmed: {trim_name}")
        trim_name = 0
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
    print("-----\nCONFIG 2 Execution")
    print(f'INFO  :  entry_group - "{entry_group}"')
    print(f'INFO  :  bucket_name - "{bucket_name}"')
    print(
        f'INFO  :  Config 2 files path: "{config_file}/data_catalog_create_ext_entries_config.txt"'
    )
    print("INFO  :  Creating/Updating...data_catalog_create_ext_entries_config.txt")
    count = 0
    if len(locations) == 0:
        for name in entryNames:
            locations.append(
                f"gs://{bucket_name}/{feedname}_{trouxid}/{trouxid}/{name.split('.')[-1]}/"
            )
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


def config3(classification_sheet, dirpath):
    print("-----\nCONFIG 3 Execution")
    print(f'INFO  :  entry_group - "{entry_group}"')
    print(f'INFO  :  Reading..."{classification_sheet}"')
    classification = pd.read_excel(
        dirpath + "/" + classification_sheet, sheet_name="classification"
    )
    with open(
        config_file + "/data_catalog_tag_external_entries_config.txt", "w"
    ) as config3:
        print(
            f'INFO  :  Config 3 files path: "{config_file}/data_catalog_tag_external_entries_config.txt"'
        )
        print(
            "INFO  :  Creating/Updating...data_catalog_tag_external_entries_config.txt"
        )
        count = 0
        for index, row in classification.iterrows():
            ext_name = row["Database Name"] + "_" + row["Table Name"]
            sentence = f"{entry_group},{ext_name},{row['Column Name']},{row['Taxonomy'].lower()},{row['Policy Tag'].lower()}\n"
            config3.write(sentence)
            count += 1
    print(f"INFO  :  Total lines updated in config3 file: {count}")


def config4(classification_sheet, dirpath):
    print("-----\nCONFIG 4 Execution")
    bq_projectId = yaml_data.get("BQ_projectID")
    bq_datasetId = yaml_data.get("BQ_datasetID")
    print(f'INFO  :  BQ Project ID: "{bq_projectId}"')
    print(f'INFO  :  BQ Dataset ID: "{bq_datasetId}"')
    print(f'INFO  :  Reading..."{classification_sheet}"')
    all_sheets = pd.read_excel(
        dirpath + "/" + classification_sheet, header=0, sheet_name=None
    )
    sheet1 = all_sheets["classification"]
    sheet2 = all_sheets["BQ"]
    bq = open(config_file + "/data_catalog_tag_bigquery_col_config.txt", "w")
    print(
        f'INFO  :  Config 3 files path: "{config_file}/data_catalog_tag_bigquery_col_config.txt"'
    )
    print("INFO  :  Creating/Updating...data_catalog_tag_bigquery_col_config.txt")
    count = 0
    for indexbq, rowbq in sheet2.iterrows():
        name = rowbq["Table Name"]
        run = 0
        for index, row in sheet1.iterrows():
            if name == row["Table Name"]:
                if run == 0:
                    b = f"{bq_projectId}.{bq_datasetId}.{name},bld_{row['Taxonomy'].lower()}:{row['Policy Tag'].lower()}:{row['Column Name']}:add"
                    run += 1
                    continue
                b = (
                    b
                    + f";bld_{row['Taxonomy'].lower()}:{row['Policy Tag'].lower()}:{row['Column Name']}:add"
                )
        count += 1
        b = b + "\n"
        bq.write(b)
    bq.close()
    print(f"INFO  :  Total lines updated in config4 file: {count}")


def check_path(destination, config1):
    if not os.path.exists(destination):
        print(f'ERROR :  Path dose not exist, "{destination}"')
        if config1 == "config1":
            print("ERROR :  Mention the correct Atlas Json path to proceed")
            print("*******\nEXECUTION FAILED")
            restore_stdout(stdout_original)
            exit()
        print(f"INFO  :  Creating directory...")
        os.makedirs(destination)
        print(f'INFO  :  Path directory created, "{destination}"')


if __name__ == "__main__":
    log_file = "execution.log"
    setup_logging(log_file)
    stdout_original = sys.stdout
    redirect_stdout_to_file(log_file)

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
        "boolean",
    ]
    yaml_file = "parameter.yaml"
    yaml_data = read_yaml_file(yaml_file)
    feedname = yaml_data.get("feedname")
    trouxid = yaml_data.get("trouxid")
    database = yaml_data.get("schema_database")
    rawDB = yaml_data.get("raw_database")
    hqlFile = yaml_data.get("hql")
    create_catalogJson = bool(yaml_data.get("create_catalogJson").lower() == "true")
    config_file = r"./Configs/" + feedname
    config_1 = bool(yaml_data.get("config1").lower() == "true")
    config_2 = bool(yaml_data.get("config2").lower() == "true")
    config_3 = bool(yaml_data.get("config3").lower() == "true")
    config_4 = bool(yaml_data.get("config4").lower() == "true")
    entry_group = yaml_data.get("entry_group")
    bucket_name = yaml_data.get("bucket_name")
    classification_sheet = yaml_data.get("classification_sheet")
    GCP_CatalogJson_path = yaml_data.get("GCP_CatalogJson_path")
    GCP_AtlasJson_path = yaml_data.get("GCP_AtlasJson_path")

    print("\nEXECUTION STARTED")
    print("*******")
    print(f"Feed Name: {feedname}")
    print(f"TrouxID: {trouxid}")
    print(f"Schema Database: {database}")
    print(f"Raw Database: {rawDB}")
    print(f"HQL: {hqlFile}")
    print("*******")
    print("OPERATIONS:-")
    print(f"create_catalogJson: {create_catalogJson}")
    print(f"config_1: {config_1}")
    print(f"config_2: {config_2}")
    print(f"config_3: {config_3}")
    print(f"config_4: {config_4}\n-----")
    if create_catalogJson:
        destination = r"./CatalogJson/" + feedname
        print(f'INFO  :  Catalog_Json_Path: "{destination}"')
        check_path(destination, "")
    elif config_1:
        destination = yaml_data.get("atlasJson_path")
        print(f'INFO  :  Atlas_Json_Path: "{destination}"')
        check_path(destination, "config1")
    if config_2 or config_3 or config_4:
        check_path(config_file, "")
    if create_catalogJson:
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
    elif config_1:
        print("-----\nCONFIG 1 Execution")
        if len(os.listdir(destination)) == 0:
            print(f"ERROR :  The directory is empty, {destination}")
            print("*******\nEXECUTION FAILED")
            restore_stdout(stdout_original)
            exit()
        print(
            f'INFO  :  Config files path: "{config_file}/data_catalog_create_schema_json_config.txt"'
        )
        config1(destination, config_file)
    if config_3 or config_4:
        source = r"./classification"
        checkSheet = True
        for dirpath, dirnames, filenames in os.walk(source):
            for file in filenames:
                if file == classification_sheet:
                    checkSheet = False
                    print(f'INFO  :  "{file}" found at "{dirpath}"')
                    if config_3:
                        config3(classification_sheet, dirpath)
                    if config_4:
                        config4(classification_sheet, dirpath)
        if checkSheet:
            print(f'ERROR :  "{classification_sheet}" dose not exist')
            print("EXECUTION FAILED\n")
            exit()
    elif not create_catalogJson or not config_1 or not config_2:
        if config_2:
            print(
                'ERROR :  To execute the "config2", either "create_catalogjson" or "config_1" should be "True"'
            )
        else:
            print(
                "ERROR :  No operation specified to perform (Please check parameter.yaml)"
            )
        print("EXECUTION FAILED\n")
        exit()
    print("*******\nEXECUTION COMPLETED")

    restore_stdout(stdout_original)
