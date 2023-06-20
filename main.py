import pandas as pd
from gespla import download, load
import os
import dbConfigs as dbc
import mysql.connector
from datetime import datetime
def main():
    # update_downloaded_data()

    update_flow_bd('75')

def update_flow_bd(subBasinCod):
    current_path = os.getcwd()
    flowFilesNames = os.listdir(current_path + "\\flow_files") #list

    i = 0
    for file in flowFilesNames:
        df = pd.read_csv(current_path + "\\flow_files\\" + file)
        serie = df.items()
        try:
            codStation = file[9:17]  # pronto
            dbc.insert_flowstations(codStation=codStation, name='TESTE ' + str(i), subBasinCod=subBasinCod)
        except Exception as e:
            print(e)
        i = i + 1
        for tuple in serie:
            date, flow = date_flow(tuple)
            if flow != "":
                try:
                    dbc.insert_measuresflow(date=date, codstation=codStation, flow=flow)
                except Exception as e:

                    print(e)


def date_flow(tuple: tuple):
    """

    """

    serie1 = tuple[1]
    string = serie1[0]
    date = string[0:10]
    flow = string[11:]

    return date, flow

def update_downloaded_data():
    current_path = os.getcwd()
    dir_flow = 'flow_files'
    dir_metadata = current_path + "\\metadata"
    # dir_prec = 'prec_files'

    mkdir_del(dir_metadata)

    meta_flow = download.metadata_ana_flow(folder=dir_metadata)

    # dataframePandas
    df_meta_flow = load.metadata_ana_flow(file=meta_flow)

    basin = 75  # int(input("Código da bacia desejada: "))

    # filtra estações cuja SubBasin seja igual ao código do input
    # ex: 75 -> Bacia Uruguai
    list_stations = df_meta_flow.loc[df_meta_flow['SubBasin'] == basin]

    # separa os códigos das estações
    stations_code = list_stations.loc[:, "CodEstacao"]

    if list_stations.empty:
        print("Nenhuma estação encontrada para o código de bacia digitado\nAbortando programa")
        return

    stations_code = list(stations_code)

    mkdir_del(dir_flow)

    try:
        download_flow_data(stations_code=stations_code, path=current_path + '\\' + dir_flow)
    except Exception as e:
        print(e)
    else:
        print("TRANQUILO")


def mkdir_del(path: str):
    """
    Creates a new directory with the specified name if it does not exist.
    If it exists, all files inside it are deleted.

    :param path: string with path/dir name
    """
    try:
        os.mkdir(path)
    except:
        print(path+' directory already exists')
        for files in os.scandir(path):
            os.remove(files.path)
        print("Pre existing files in directory have been deleted")


def downloadPrecData(stations_code: list, path: str):
    """
    Downloads all precipitaiton information from the stations specified

    Each stations data is saved in individual .txt files

    :param stations_code: list
    :param path: string with path to store files
    """
    for i in range(len(stations_code)):
        file_prec = download.ana_prec(code=stations_code[i], folder=path)
        print('Arquivo salvo em: {}'. format(file_prec))


def download_flow_data(stations_code: list, path: str):
    """
    Downloads all flow information from the stations
    specified.

    Each station data is saved in individual .txt files.

    :param stations_code: list
    :param path: string with path to store files
    """
    for i in range(len(stations_code)):
        file_flow = download.ana_flow(code=stations_code[i], folder=path)
        print('Arquivo salvo em: {}'.format(file_flow))


if __name__ == '__main__':
    # try:
    main()
    # except Exception as e:
        # print(e)