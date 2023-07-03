import dbConfigs as dbc
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import geopandas as gpd
import geodatasets

def main():
    tempo2()

    #measureGraph(75770000)

def temporario():
    x = np.random.rand(20)
    y = np.random.rand(20)
    colors = np.random.randint(1, 5, size=len(x))

    fig, ax = plt.subplots()

    scatter = plt.scatter(
        x=x,
        y=y,
        s=100,
    )
    plt.plot()
    plt.show()

def tempo2():
    shp = gpd.read_file("C:\\Users\\migbr\\OneDrive\\Documentos\\Eco\\ecoDownload\\shapefiles\\BR_UF_2021.shp")
    print(type(shp))

def getCodStations():
    """
    Retorna uma lista com todos os códigos de Estações do Banco
    """
    db = dbc.db_connection(host='localhost', user='root', password='root', database='eco')
    cursor = db.cursor()

    command = "SELECT distinct codStation FROM measuresFlow"
    cursor.execute(command)
    rows = cursor.fetchall()

    for i in range(len(rows)):
        rows[i] = rows[i][0]
        print(type(rows[i]))
    return rows

def measureGraph(codStation: int):
    retorno = dateflow(codStation)
    datas = list()
    medidas = list()
    for i in range(len(retorno)):
        datas.append(retorno[i][0])
        medidas.append(retorno[i][1])

    datas = np.array(datas)
    medidas = np.array(medidas)

            # x,y
    plt.plot(datas, medidas)
    plt.show()


def dateflow(codStation: int):
    db = dbc.db_connection(host='localhost', user='root', password='root', database='eco')
    cursor = db.cursor()

    command = "SELECT date, flow FROM measuresFlow WHERE codStation = "+str(codStation)
    cursor.execute(command)
    rows = cursor.fetchall()

    return rows

if __name__ == '__main__':
    main()
