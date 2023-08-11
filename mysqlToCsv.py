import os
import dbConfigs as dbc

def write_line(filePath: str, data: dict):
    """
    :param filePath: path to file and file name/extension
    :param data: dict in order of data to be written

    """

    file = open(filePath, "a")
    line = str()
    for key in data:
        line = line + str(data[key]) + ";"

    line = line+"\n"
    file.write(line)


def create_file(filePath: str, collumns: list):
    file = open(filePath, "x")
    for collumn in collumns:
        item = collumn+";"
        file.write(item)
    file.write("\n")


def getData(column: str, database: str,table: str):
    db = dbc.db_connection(host='localhost', user='root', password='root', database='eco')
    cursor = db.cursor()
    return dbc.select_row(cursor, column=column, database=database, table=table)


def measuresCSV(current_path: str, type: str):
    """
    :param current_path: directory where .csv will be saved
    :param type: only prec (preciptation) or flow
    """

    if type != 'flow' and type != 'prec':
        raise Exception("Type not supported")
    path = current_path + "\\csv\\"+"measures"+type+".csv"
    try:
        os.remove(path)
    except Exception as e:
        print(e)

    try:
        create_file(path, collumns=["date", "codStation", "flow"])
    except Exception as e:
        print(e)

    data = getData("*", "eco", "measures"+type)

    info = dict()
    for item in data:
        info["date"] = item[0]
        info["cod"] = item[1]
        info["flow"] = item[2]
        write_line(path, info)


def stationCSV(current_path: str, type: str):
    """
    :param current_path: directory where .csv will be saved
    :param type: only prec (preciptation) or flow
    """

    if type != 'flow' and type != 'prec':
        raise Exception("Type not supported")

    try:
        path = current_path+"\\csv\\"+type+"stations"+".csv"
        os.remove(path)
    except Exception as e:
        print(e)
    try:
        create_file(path, collumns=["codStation", "name", "latitude", "longitude"])
    except Exception as e:
        print(e)

    data = getData("*", "eco", type+"Stations")
    info = dict()
    for item in data:
        info["cod"] = item[0]
        info["name"] = item[1]
        info["lat"] = item[3]
        info["lon"] = item[4]
        write_line(path, info)


def main():

    current_path = os.getcwd()

    try:
        os.mkdir("csv")
    except Exception as e:
        print(e)

    measuresCSV(current_path, "flow")
    measuresCSV(current_path, "prec")

    stationCSV(current_path, type="flow")
    stationCSV(current_path, type="prec")


if __name__ == '__main__':
    main()
