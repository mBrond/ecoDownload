import os
import dbConfigs as dbc

def write_line(filePath: str, data: dict):
    file = open(filePath, "a")
    line = str(data["cod"])+";"+data["name"]+";"+str(data["lat"])+";"+str(data["lon"])+"\n"
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

def flowStationCSV(current_path: str):

    path = current_path+"\\flowstations.csv"
    os.remove(path)
    try:
        create_file(path, collumns=["codStation", "name", "latitude", "longitude"])
    except Exception as e:
        print(e)

    data = getData("*", "eco", "flowStations")
    info = dict()
    for item in data:
        info["name"] = item[1]
        info["lat"] = item[3]
        info["lon"] = item[4]
        info["cod"] = item[0]
        write_line(path, info)


def flowMeasuresCSV(current_path: str):
    path = current_path + "\\measuresflow.csv"
    try:
        os.remove(path)
    except Exception as e:
        print(e)

    try:
        create_file(path, collumns=["date", "codStation", "flow"])
    except Exception as e:
        print(e)

    data = getData("*", "eco", "measuresflow")

    info = dict()
    for item in data:
        info["date"] = item[0]
        info["codStation"] = item[1]
        info["flow"] = item[2]

def main():

    current_path = os.getcwd()
    flowStationCSV(current_path)
    flowMeasuresCSV(current_path)

if __name__ == '__main__':
    main()
