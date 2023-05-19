import hydrobr
from gespla import download, load
import os
import shutil

def main():
    current_path = os.getcwd()
    dir_flow = 'flow_files'
    dir_metadata = current_path+"\\metadata"
    # dir_prec = 'prec_files'

    mkdir_del(dir_metadata)

    meta_flow = download.metadata_ana_flow(folder=dir_metadata)

    # dataframePandas
    df_meta_flow = load.metadata_ana_flow(file=meta_flow)

    basin = 75 #int(input("Código da bacia desejada: "))

    #filtra estações cuja SubBasin seja igual ao código do input
    # ex: 75 -> Bacia Uruguai

    list_stations = df_meta_flow.loc[df_meta_flow['SubBasin'] == basin]

    #separa os códigos das estações
    stations_code = list_stations.loc[:, "CodEstacao"]

    if list_stations.empty:
        print("Nenhuma estação encontrada para o código de bacia digitado\nAbortando programa")
        return

    stations_code = list(stations_code)

    mkdir_del(dir_flow)

    try:
        downloadFlowData(stations_code=stations_code, path=current_path+'\\'+dir_flow)
    except Exception as e:
        print(e)
    else:
        print("TRANQUILO")

    return
def mkdir_del(path):
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


def downloadPrecData(stations_code, path):
    """
    Downloads all precipitaiton information from the stations specified

    Each stations data is saved in individual .txt files

    :param stations_code: list
    :param path: string with path to store files
    """
    for i in range(len(stations_code)):
        file_prec = download.ana_prec(code=stations_code[i], folder=path)
        print('Arquivo salvo em: {}'. format(file_prec))

def downloadFlowData(stations_code, path):
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
    try:
        main()
    except Exception as e:
        print(e)