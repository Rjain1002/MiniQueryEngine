import sys
import os
import re
import csv
# sys.path.insert(0, os.getcwd() + "/sqlparse-0.2.4")
import sqlparse

table_schema = {}
queryColumn = []
tables = []
natjoin = []

def readMetaData():
    fp = open("metadata.txt", "r")
    flag = 0
    for line in fp:
        line = line.strip()
        if line == '<begin_table>':
            columns = []
            flag = 1
        elif flag == 1:
            tablename = line
            flag = 0
        elif line == '<end_table>':
            table_schema[tablename] = columns
        else:
            columns.append(line)
    fp.close()

def findColIndex(col):
    ret = -1
    n = len(queryColumn)
    for i in range(n):
        var = queryColumn[i]
        if col.lower() == var.lower():
            ret = i
        elif var.lower().endswith("." + col):
            ret = i
    return ret

def queryColumns(tablenames):
    query_cols = []
    for name in tablenames:
        if name in table_schema:
            schema = table_schema[name]
            for colname in schema:
                query_cols.append(name + "." + colname)
        else:
            break
    return query_cols

def validColnameAndAggregate(cols, column, aggregate):
    cols = cols.split(",")
    errDist = 0
    errCol = 0
    n = len(cols)
    for i in range(n):
        c = cols[i].strip()
        if c.lower().startswith(("max", "min", "sum", "avg")):
            if errCol == 1:
                return -1
            else:
                aggregate.append(c.split("(")[0])
                column.append(c[4:len(c)-1])
                errDist = 1
        else:
            if errDist == 1:
                return -1
            else:
                column.append(c)
                errCol = 1
    return 0

def readTable(name, distflag):
    name = name + ".csv"
    table = []
    try:
        reader = csv.reader(open(name), delimiter=',')
    except Exception as e:
        print("Query not formed properly")
        sys.exit()
    for row in reader:
        for i in range(len(row)):  
            if row[i][0] in ["'", '"']:
                row[i] = row[i][1:-1]
        row = list(map(int, row))
        if distflag == 1:
            if row not in table:
                table.append(row)
        else:
            table.append(row)
    return table

def getColumnNo(columns):
    resCol = []
    if len(queryColumn) == 0:
        return resCol
    if columns[0] == '*' and len(columns) == 1:
        columns = queryColumn
    for col in columns:
        try:
            resCol.append(queryColumn.index(col))
        except ValueError:
            flag = 0
            colid = 0
            n = len(queryColumn)
            for i in range(n):
                var = queryColumn[i]
                if var.endswith("." + col):
                    flag += 1
                    colid = i
            if flag == 1:
                resCol.append(colid)
            else:
                return []
    k = len(natjoin)            
    if k > 0:
        for i in range(k):
            if natjoin[i][0] in resCol and natjoin[i][1] in resCol:
                i1 = resCol.index(natjoin[i][0])
                i2 = resCol.index(natjoin[i][1])
                if i1 < i2:
                    del resCol[i2]
                else:
                    del resCol[i1]
    return resCol

def normalHeading(rescol):
    heading = []
    for i in rescol:
        heading.append(queryColumn[i])
    heading = ",".join(heading)
    heading += '\n'
    return heading

def getaggrHeading(column, aggregate):
    heading = ""
    co = getColumnNo(column)
    if len(co) == 0:
        print("Syntax error in columns")
        sys.exit()
    for i in range(len(co)):
        heading += aggregate[i] + "(" + queryColumn[co[i]] + "),"
    heading = heading[:-1]
    heading = heading + '\n'
    return heading        

def join(tablenames, distflag):
    n = len(tablenames)
    if n == 1:
        return readTable(tablenames[0].strip(), distflag)
    else:
        tablenames = list(map(str.strip, tablenames))
        k = 0
        table = []
        for name in tablenames:
            if k == 0:
                table = readTable(name, distflag)
            else:
                t = readTable(name, distflag)
                tlen = len(t)
                temp = []
                tablelen = len(table)
                for i in range(0, tablelen):
                    for j in range(0, tlen):
                        temp.append(table[i] + t[j])
                table = temp
            k = k + 1
    return table

def conSplit(condition):
    try:
        delimiters = "and", "or"
        regexPattern = '|'.join(map(re.escape, delimiters)) + "(?i)"
        con = re.split(regexPattern, condition)
        con = list(map(str.strip, con))
    except Exception as e:
        print("Syntax of and/or condition not correct")
        sys.exit()
    return con

def getOperands(con):
    operand = []
    try:
        relop = ""
        i = 0
        while i < len(con):
            if con[i] == '>' and con[i+1] == '=':
                relop = ">="
                i += 1
            elif con[i] == '>' and con[i+1] != '=':
                relop = ">"
                i += 1
            elif con[i] == '<' and con[i+1] == '=':
                relop = "<="
                i += 1
            elif con[i] == '<' and con[i+1] != '=':
                relop = "<"
                i += 1
            elif con[i] == '!' and con[i+1] == '=':
                relop = "!="
                i += 1
            elif con[i] == '=' and con[i+1] not in ['=', '<', '>', '!']:
                relop = "="
                i += 1
            i += 1
        operand = con.split(relop)
        operand = list(map(str.strip, operand))
        if relop != "=":
            return operand, relop
        else:
            return operand, "=="
    except:
        print("Syntax Error in operand")
        sys.exit()
    return operand, relop

def naturalJoin(condition):
    global natjoin
    try:
        con = conSplit(condition)
        for var in con:
            operand, operator = getOperands(var)
            operand = list(map(str.strip, operand))
            if '.' in operand[0] and '.' in operand[1]:
                if operator.strip() == "==":
                    sameCol = findColIndex(operand[0].strip()), findColIndex(operand[1].strip())
                    natjoin.append(sameCol)
    except Exception as e:
        print("Syntax Error in operand, operator around each (and/or) operator")
        sys.exit()

def evaluateWhereValueBased(condition):
    try:
        connector = []
        cond = condition.split(" ")
        cond = list(map(str.strip, cond))
        n = len(cond)
        for i in range(n):
            temp = cond[i].lower().strip()
            if temp == "and" or temp == "or":
                connector.append(temp)
        con = conSplit(condition)
        i = 0
        for var in con:
            operand, operator = getOperands(var)
            lhs = findColIndex(operand[0].strip())
            rhs = findColIndex(operand[1].strip())
            if lhs > -1 and rhs > -1:
                operand[0] = operand[0].replace(operand[0], "tables[i][" + str(lhs) + "]")
                operand[1] = operand[1].replace(operand[1], "tables[i][" + str(rhs) + "]")
            elif lhs > -1:
                operand[0] = operand[0].replace(operand[0], "tables[i][" + str(lhs) + "]")
            else:
                print("Syntax error in getting condition in findColIndex in evalWhereValues")
                sys.exit()
            t = operand[0], operand[1]
            con[i] = operator.join(t)
            i += 1
        x = 0
        j = 0
        newcon = ""
        for var in con:
            if j == 0:
                newcon = var + " "
            else:
                conj = connector[0]
                newcon = newcon + conj + " "
                newcon = newcon + var + " "
            j += 1
            x += 1
        res = []
        for i in range(len(tables)):
            if eval(newcon):
                res.append(tables[i])
    except Exception as e:
        print("Syntax Error in operands or operators")
        sys.exit()
    return res

def calAggr(columns):
    colResult = []
    for i in columns:
        temp = []
        colNo = findColIndex(i)
        for j in range(len(tables)):
            temp.append(tables[j][colNo])
        colResult.append(temp)
    return colResult

def getcolumn(tables, ind):
    if len(tables) == 0 or len(tables[0]) <= ind or ind == 0:
        print("Error in columns specified")
        sys.exit()
    result = []
    for i in ind:
        temp = []
        for j in range(len(tables)):
            temp.append(tables[j][i])
        result.append(temp)
    return result

def processQuery():
    readMetaData()
    
    # Check if the script received a command line argument
    if len(sys.argv) < 2:
        print("Error: No query file provided.")
        sys.exit()

    f = sys.argv[1]
    
    try:
        with open(f, "r") as file:
            sql = file.read()
    except FileNotFoundError:
        print(f"Error: File '{f}' not found.")
        sys.exit()
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit()

    sql = sqlparse.format(sql, keyword_case='upper')
    query = sqlparse.parse(sql)[0]
    query = sqlparse.sql.IdentifierList(query.tokens).get_identifiers()
    
    if len(query) != 2 and len(query) != 4:
        print("Syntax error in query format")
        sys.exit()
    
    aggregate = []
    columns = []
    distinct = 0
    table_names = []

    if query[0].ttype is sqlparse.tokens.DML and query[0].value.upper() == 'SELECT':
        s = query[1]
        if query[2].ttype is sqlparse.tokens.Keyword and query[2].value.upper() == 'FROM':
            t = query[3]
        else:
            print("Syntax error in query format")
            sys.exit()
    else:
        print("Syntax error in query format")
        sys.exit()

    if s.get_name() is None:
        s = s.get_real_name()
    else:
        s = s.get_name()

    if s.lower().startswith("distinct"):
        distinct = 1
        s = s[8:]

    if validColnameAndAggregate(s, columns, aggregate) != 0:
        print("Syntax error in columns")
        sys.exit()

    table_names = t.get_name().split(",")
    table_names = list(map(str.strip, table_names))

    if distinct == 1:
        tables = join(table_names, 1)
    else:
        tables = join(table_names, 0)

    queryColumn = queryColumns(table_names)
    where = 0
    
    if 'WHERE' in sql:
        where = 1
        wh = sql.split("WHERE")
        wh = wh[1].strip()
        wh = wh[:len(wh)-1]
        naturalJoin(wh)
        tables = evaluateWhereValueBased(wh)

    rescol = getColumnNo(columns)
    if len(rescol) == 0:
        print("Syntax error in columns")
        sys.exit()

    if where == 0 and len(columns) == 0 and len(aggregate) == 0:
        print("Error in selecting columns")
        sys.exit()

    if len(aggregate) == 0:
        if len(columns) == 1 and columns[0] == '*':
            heading = normalHeading(queryColumn)
        else:
            heading = normalHeading(rescol)
        print(heading, end="")
        for row in tables:
            result = []
            for i in rescol:
                result.append(row[i])
            result = ",".join(map(str, result))
            print(result)
    else:
        colResult = calAggr(columns)
        res = []
        i = 0
        for var in aggregate:
            if var.lower() == "max":
                res.append(max(colResult[i]))
            elif var.lower() == "min":
                res.append(min(colResult[i]))
            elif var.lower() == "sum":
                res.append(sum(colResult[i]))
            elif var.lower() == "avg":
                res.append(sum(colResult[i]) / len(colResult[i]))
            i += 1
        heading = getaggrHeading(columns, aggregate)
        print(heading, end="")
        print(",".join(map(str, res)))

processQuery()


