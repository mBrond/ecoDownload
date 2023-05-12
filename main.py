import hydrobr
from gespla import download, load
import os
import shutil

def main():
    dir_flow = 'flow_files'
    try:
        os.mkdir('metadata')
    except:
        print('"metadata" directory already exists')

    current_path = os.getcwd()

    meta_flow = download.metadata_ana_flow(folder=current_path+"\\metadata")

    #dataframePandas
    df_meta_flow = load.metadata_ana_flow(file=meta_flow)

    #filtra estações cuja SubBasin seja 75 --> subBacias Uruguai
    list_stations = df_meta_flow.loc[df_meta_flow['SubBasin'] == 75]

    #separa os códigos das estações
    stations_code = list_stations.loc[:, "CodEstacao"]
    stations_code = list(stations_code)

    try:
        os.mkdir(dir_flow)
    except:
        shutil.rmtree(dir_flow)

    downloadFlowData(stations_code=stations_code, path=current_path+'\\'+dir_flow)


def downloadFlowData(stations_code, path):
    'list: station_code, string: path'
    for i in range(len(stations_code)):
        file_flow = download.ana_flow(code=stations_code[i], folder=path)
        print('Arquivo salvo em: {}'.format(file_flow))

if __name__ == '__main__':

    main()