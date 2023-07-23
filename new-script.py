import os
import re

rootdir=r'./hql'
outputdir=r'./jsonDatacatalog'
schemaName = "consumption_rf_3_insfin_verona_mpf"

print(rootdir)

start_flag = False
def hql_file(subdir, file):
    curfile = open(os.path.join(subdir,file), "r", errors='ignore')
    for ind,i in enumerate(curfile):
        global write_json
        global start_flag
        if "create " in i.lower():
            if ind!=0 and start_flag:
                #write_json.write("]\n")
                write_json.write("\n]}\n}")
            table_nm = i.lstrip().rstrip().replace("`", "").split(" ")
            table_nm1 = [t.split('\t')[-1] for t in table_nm]
            #name = table_nm1[-1].replace(".", "_")
            name = table_nm1[-1].split(".")
            filename = schemaName + "." + name[-1].replace("(", "") + ".json".lower()
            dcTblNm = filename.replace(".json", "")
            write_json = open(os.path.join(outputdir, filename), "w", errors='ignore')
            write_json.write("{\n")
            write_json.write(f"\"name\": \"{dcTblNm}\",\n")
            write_json.write("\"description\": \"\",\n")
            write_json.write(f"\"displayName\": \"{dcTblNm}\",\n")
            write_json.write("\"linked_resource\": \"\",\n")
            write_json.write("\"schema\": {\"columns\":[")
            #write_json.write("[\n")
            start_flag=True
        #write_json = open(os.path.join(outputdir, filename), "w", errors='ignore')
        #if j.startwith(").
        j = i.lstrip().rstrip().replace('`', '').split(" ")
        if j[0].startswith('"'):
            #j = j.strip().replace(' ', '')
            #j = re.sub('"', '', j[0])
            j = j[2:]
            #j = j[0].strip('"')
            #j = [y.strip('" ').replace(" ", "") for y in j]
            #print(j)
        if "partitioned" in i.lower():
            for ind,word in enumerate(j):
                if "int" in word.lower():
                    s1="INTEGER"
                    mode="NULLABLE"
                    col_nm = j[ind-1]
                    if "(" in col_nm:
                        temp_list = col_nm.split("(")
                        col_nm = temp_list[-1]
                    write_json.write("""   {
                    \"column\": \"""" + col_nm + """\",
                    \"type\": \"""" + s1 + """\",
                    \"mode\": \"""" + mode + """\",
                    \"description\": \"""" + col_nm + """\"\n   },\n""")
        else:
            if " char" in i.lower() or " string" in i.lower() or " varchar" in i.lower():
                s1 = "STRING"
                chk = True
            elif " int" in i.lower():
                s1 = "INTEGER"
                chk = True
            elif " numeric" in i.lower() or " decimal" in i.lower():
                s1 = "NUMERIC"
                chk = True
            elif " float" in i.lower() or " double" in i.lower():
                s1 = "FLOAT"
                chk = True
            elif " timestamp" in i.lower():
                s1 = "TIMESTAMP"
                chk = True
            elif " boolean" in i.lower():
                s1 = "BOOLEAN"
                chk = True
            elif " tinyint" in i.lower():
                s1 = "INTEGER"
                chk = True
            elif " smallint" in i.lower():
                s1 = "INTEGER"
                chk = True
            elif " bigint" in i.lower():
                s1 = "INTEGER"
                chk = True
            elif " binary" in i.lower():
                s1 = "BYTES"
                chk = True
            elif " date" in i.lower():
                s1 = "DATE"
                chk = True
            else:
                chk = False
            # if "comment" in i.lower():
            #     cmtval=""
            #     x = [item.lower() for item in j].index("comment")
            #     cmtchk = True
            #     for val in range(x+1, len(j)):
            #         cmtval=cmtval+" "+j[val]
            # else:
            #     cmtchk = False
            if " primary" in i.lower():
                mode="REQUIRED"
            if "is not null" in i.lower():
                mode="REQUIRED"
            if chk:
                mode = "NULLABLE"
                try:
                    # if cmtchk:
                    #     cmtval = cmtval.replace("'","")
                    #     des_val = cmtval.replace(",","").strip()
                    # else:
                    des_val = str(j[0]).lower()

                    write_json.write("""   {
                    \"column\": \"""" + j[0] + """\",
                    \"type\": \"""" + s1 + """\",
                    \"mode\": \"""" + mode + """\",
                    \"description\": \"""" + des_val + """\"\n   },\n""")

                except IndexError:
                    print(j)
            elif chk:
                write_json.write(""" {
                \"column\": \""""+str(j[0][1:-1]).lower()+"""\",
                \"type\": \""""+s1+"""\",
                \"mode\": \""""+mode+"""\",
                \"description\": \""""+str(j[0][1:-1]).lower()+"""\"         },\n""")
    
    write_json.close()
    start_flag=False


for subdir, dirs, files in os.walk(rootdir):
    for file in files:
        if ".hql" in file or ".csv" in file:
            hql_file(subdir, file)
        else:
            pass
