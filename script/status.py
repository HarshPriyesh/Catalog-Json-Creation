def fail():
    print("Execution Complete")
    print("STATUS: FAILED\n")
    exit()


def passed(count):
    print("Execution Complete")
    print(f"INFO  :  Total number of catalog json created- {count}")
    print("STATUS: SUCCESS\n")
