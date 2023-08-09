from gespla import download, load
import os
import dbConfigs as dbc

def main():
    subBasinCode = 75 # Rio Uruguai

    try:
        #update_prec_downloaded_data(subBasinCode)
        update_prec_bd(subBasinCode)
    except Exception as e:
        print(e)

    try:
        #update_flow_downloaded_data(subBasinCode)
        update_flow_bd(subBasinCode)
    except Exception as e:
        print(e)


def update_flow_bd(subBasinCod):
    current_path = os.getcwd()
    flowFilesNames = os.listdir(current_path + "\\flow_files") #list

    db = dbc.db_connection(host='localhost', user='root', password='root', database='eco')
    cursor = db.cursor()

    for file in flowFilesNames:
        try:
            codStation = file[9:17]
            info = get_flow_stations_info(codStation)
            name = info['name']
            lon = info['longitude']
            lat = info['latitude']
            dbc.insert_flowstations(codStation=codStation, name=name, subBasinCod=subBasinCod, latitude=lat,
                                    longitude=lon, cursor=cursor, db=db)
            print("Insertion of station "+codStation+" done.")
        except Exception as e:
            print(e)
        with open(current_path + "\\flow_files\\"+file, "r") as f:
            i=0
            for line in f:
                date = line[0:10]
                flow =line[11:].strip()
                if flow != "":
                    try:
                        dbc.insert_measuresflow(date=date, codstation=str(codStation), flow=flow, cursor=cursor, db=db)
                    except Exception as e:
                        print(e)


def update_prec_bd(subBasinCod: int):
    current_path = os.getcwd()
    precFilesNames = os.listdir(current_path + "\\prec_files") #list

    db = dbc.db_connection(host='localhost', user='root', password='root', database='eco')
    cursor = db.cursor()

    for file in precFilesNames:
        try:
            codStation = file[9:17]
            info = get_prec_stations_info(codStation)
            name = info['name']
            lon = info['longitude']
            lat = info['latitude']
            dbc.insert_precstations(codStation=codStation, name=name, subBasinCod=str(subBasinCod), latitude=lat,
                                    longitude=lon, cursor=cursor, db=db)
            print("Insertion of station "+codStation+" done.")
        except Exception as e:
            print(e)
        with open(current_path + "\\prec_files\\"+file, "r") as f:
            for line in f:
                date = line[0:10]
                precipitation =line[11:].strip()
                if precipitation != "":
                    precipitation = "NULL"
                try:
                    dbc.insert_measuresprec(date=date, codstation=codStation, precipitation=precipitation, cursor=cursor, db=db)
                except Exception as e:
                    print(e)


def get_prec_stations_info(codStation: str):
    current_path = os.getcwd()

    dir_metadata = current_path + "\\metadata\\meta_prec"

    metadata_file = os.listdir(dir_metadata)[0] #retorna lista com 1 elemento -> nome do arquivo de metadata

    df_meta_prec = load.metadata_ana_prec(file=dir_metadata+'\\'+metadata_file) #df do metadata

    data = df_meta_prec.loc[df_meta_prec['CodEstacao']==str(codStation)] #data = df

    # .loc retorna uma serie, .tolist converte para lista com um item
    name = data.loc[:, 'Name'].tolist()[0]
    lon = str(data.loc[:, 'Longitude'].tolist()[0])
    lat = str(data.loc[:, 'Latitude'].tolist()[0])

    info={
        'name': name,
        'longitude': lon,
        'latitude': lat
    }

    return info


def get_flow_stations_info(codStation: str):
    current_path = os.getcwd()

    dir_metadata = current_path + "\\metadata\\meta_flow"

    metadata_file = os.listdir(dir_metadata)[0] #retorna lista com 1 elemento -> nome do arquivo de metadata

    df_meta_flow = load.metadata_ana_flow(file=dir_metadata+'\\'+metadata_file) #df do metadata

    data = df_meta_flow.loc[df_meta_flow['CodEstacao']==str(codStation)] #data = df

    # .loc retorna uma serie, .tolist converte para lista com um item
    name = data.loc[:, 'Name'].tolist()[0]
    lon = str(data.loc[:, 'Longitude'].tolist()[0])
    lat = str(data.loc[:, 'Latitude'].tolist()[0])

    info={
        'name': name,
        'longitude': lon,
        'latitude': lat
    }

    return info


def update_prec_downloaded_data(subBasinCode: int):
    current_path = os.getcwd()
    dir_metadata = current_path + "\\metadata\\meta_prec"
    dir_prec = 'prec_files'

    # mkdir_del(dir_metadata)

    meta_prec = download.metadata_ana_prec(folder=dir_metadata)
    # meta_prec = "C:\\Users\\migbr\\OneDrive\\Documentos\\Eco\\ecoDownload\\metadata\\meta_prec\\metadata_ANA-prec_2023-07-05.txt"

    # dataframePandas
    df_meta_prec = load.metadata_ana_flow(file=meta_prec)
    list_prec_stations = df_meta_prec.loc[df_meta_prec['SubBasin'] == subBasinCode]

    stations_code = list_prec_stations.loc[:, "CodEstacao"]
    stations_code = list(stations_code)

    if list_prec_stations.empty:
        print("Nenhuma estação encontrada para o código de bacia digitado (Precipitação)\nAbortando programa")
        return

    mkdir_del(dir_prec)

    try:
        download_prec_data(stations_code=stations_code, path=current_path + '\\' + dir_prec)
    except Exception as e:
        print(e)


def update_flow_downloaded_data(subBasinCode: int):
    current_path = os.getcwd()
    dir_flow = 'flow_files'
    dir_metadata = current_path + "\\metadata\\meta_flow"
    dir_prec = 'prec_files'

    # mkdir_del(dir_metadata)

    meta_flow = download.metadata_ana_flow(folder=dir_metadata)

    # dataframePandas
    df_meta_flow = load.metadata_ana_flow(file=meta_flow)

    # filtra estações cuja SubBasin seja igual ao código do parametro
    # ex: 75 -> Bacia Uruguai
    list_flow_stations = df_meta_flow.loc[df_meta_flow['SubBasin'] == subBasinCode]

    # separa os códigos das estações
    stations_code = list_flow_stations.loc[:, "CodEstacao"]

    if list_flow_stations.empty:
        print("Nenhuma estação encontrada para o código de bacia digitado (Vazão)\nAbortando programa")
        return

    stations_code = list(stations_code)

    mkdir_del(dir_flow)

    try:
        download_flow_data(stations_code=stations_code, path=current_path + '\\' + dir_flow)
    except Exception as e:
        print(e)


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


def download_prec_data(stations_code: list, path: str):
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