from gespla import download, load
import os
import dbConfigs as dbc
import getDatas as gdt

def main():
    subBasinCode = 75 # Rio Uruguai

    update_dowloaded_data(subBasinCode, 'flow')
    update_bd(subBasinCode, 'flow')

    #update_dowloaded_data(subBasinCode, 'prec')
    update_bd(subBasinCode, 'prec')


def update_bd(subBasinCod, type: str):
    if type != 'flow' and type != 'prec':
        raise Exception("Type not supported")

    current_path = os.getcwd()
    filesNames = os.listdir(current_path + "\\"+type+"_files")  # list

    db = dbc.db_connection(host='localhost', user='root', password='root', database='eco')
    cursor = db.cursor()

    for file in filesNames:
        try:
            codStation = file[9:17]
            info = gdt.get_stations_info(codStation=codStation, type=type)
            name = info['name']
            lon = info['longitude']
            lat = info['latitude']
            if type == 'flow':
                dbc.insert_flowstations(codStation=codStation, name=name, subBasinCod=subBasinCod, latitude=lat,
                                        longitude=lon, cursor=cursor, db=db)
            elif type == 'prec':
                dbc.insert_precstations(codStation=codStation, name=name, subBasinCod=str(subBasinCod), latitude=lat,
                                        longitude=lon, cursor=cursor, db=db)
            print("Insertion of station " + codStation + " done.")
        except Exception as e:
            print(e)
        with open(current_path + "\\"+type+"_files\\" + file, "r") as f:
            for line in f:
                date = line[0:10]
                measure = line[11:].strip()
                if measure == '':
                    try:
                        if type == 'flow':
                            dbc.insert_measuresflow(date=date, codstation=str(codStation), cursor=cursor, db=db)
                        else:
                            dbc.insert_measuresprec(date=date, codstation=codStation,cursor=cursor, db=db)
                    except Exception as e:
                        print(e)
                else:
                    try:
                        if type == 'flow':
                            dbc.insert_measuresflow(date=date, codstation=str(codStation), flow=measure, cursor=cursor, db=db)
                        else:
                            dbc.insert_measuresprec(date=date, codstation=codStation, precipitation=measure,
                                                    cursor=cursor, db=db)
                    except Exception as e:
                        print(e)


def update_dowloaded_data(subBasinCod: int, type: str):
    if type != 'flow' and type != 'prec':
        raise Exception("Type not supported")

    current_path = os.getcwd()
    dir_metadata = current_path + "\\metadata\\meta_"+type
    dir = type+'_files'


    #mkdir_del(dir_metadata)

    try:
        if type == 'flow':
            meta_file = download.metadata_ana_flow(folder=dir_metadata)
        else:
            meta_file = download.metadata_ana_prec(folder=dir_metadata)
    except Exception as e:
        print('Download dos metadados do tipo "'+type+'" nao realizado: '+str(e))
        meta_file = os.listdir(dir_metadata)[0]  # only file name
        meta_file = dir_metadata+"\\" + meta_file

    # dataframePandas
    if type == 'flow':
        df_meta = load.metadata_ana_flow(file=meta_file)
    else:
        df_meta = load.metadata_ana_prec(file=meta_file)

    # filtra estações cuja SubBasin seja igual ao código do parametro
    # ex: 75 -> Bacia Uruguai
    list_stations = df_meta.loc[df_meta['SubBasin'] == subBasinCod]

    # separa os códigos das estações
    stations_code = list_stations.loc[:, "CodEstacao"]

    if list_stations.empty:
        print("Nenhuma estação encontrada para o código de bacia digitado+("+type+")\nAbortando programa")
        return

    stations_code = list(stations_code)

    mkdir_del(dir)

    try:
        download_data(stations_code=stations_code, path=current_path + '\\' + dir, type=type)
    except Exception as e:
        print("Dados das estações não realizado. Motivo: "+str(e))


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


def download_data(stations_code: list, path: str, type:str):
    """
    Downloads all flow information from the stations
    specified.

    Each station data is saved in individual .txt files.

    :param stations_code: list
    :param path: string with path to store files
    :param type: 'flow' or 'prec' (precipitation)
    """
    if type != 'flow' and type != 'prec':
        raise Exception("Type not supported")

    for i in range(len(stations_code)):
        if type=='flow':
            file = download.ana_flow(code=stations_code[i], folder=path)
        elif type =='prec':
            file = download.ana_prec(code=stations_code[i], folder=path)

        print('Arquivo salvo em: {}'.format(file))



if __name__ == '__main__':
    # try:
    main()
    # except Exception as e:
        # print(e)