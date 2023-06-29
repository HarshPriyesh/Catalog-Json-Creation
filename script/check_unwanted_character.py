from status import *


def check_unwanted_character(lines):
    for i in lines:
        space = i.count(' ')
        if space == 0 or space > 1:
            print("ERROR :  Corrupt schema file (there is no or more than one space)")
            print(f'ERROR :  At line {lines.index(i) + 1}, "{i}"')
            fail()
        bracket = 0
        for j, c in enumerate(i):
            if c == '(':
                bracket = j
            if not (c.isalnum() or c == '_' or c == ' ' or c == '(' or c == ')'):
                if not bracket > 0 and bracket < j < i.index(")") and c == ',':
                    print("ERROR :  Corrupt schema file (contain unwanted character)")
                    print(f'ERROR :  At line {lines.index(i) + 1}, "{i}"')
                    fail()
