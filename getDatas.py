import os
from gespla import load

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


def get_stations_info(codStation: str, type: str):
    """
    Returns dict with name, longitude and latitude given an station code and its type.
    Informations are taken from dataframe.
    :param codStation: Station code
    :param type: Only prec (precipitation) or flow
    """

    if type != 'flow' and type != 'prec':
        raise Exception("Type not supported")

    current_path = os.getcwd()

    dir_metadata = current_path + "\\metadata\\meta_"+type

    metadata_file = os.listdir(dir_metadata)[0] #retorna lista com 1 elemento -> nome do arquivo de metadata

    if type == 'flow':
        df_meta = load.metadata_ana_flow(file=dir_metadata + '\\' + metadata_file)  # df do metadata
    else:
        df_meta = load.metadata_ana_prec(file=dir_metadata + '\\' + metadata_file)

    data = df_meta.loc[df_meta['CodEstacao']==str(codStation)] #data = df

    # .loc retorna uma serie, .tolist converte para lista com um item
    name = data.loc[:, 'Name'].tolist()[0]
    lon = str(data.loc[:, 'Longitude'].tolist()[0])
    lat = str(data.loc[:, 'Latitude'].tolist()[0])

    return {
        'name': name,
        'longitude': lon,
        'latitude': lat
    }