import dbConfigs as dbc
import datetime
import hydrobr as hb


def get_dates():
    """
    FALTA TESTAR
    Return a list with all dates from 1/1/1940 until today
    """
    start = datetime.date(1940, 1, 1) #DATA ARBITRARIA PARA INSERCAO DOS DADOS
    end = datetime.datetime.now()
    end = end.strftime('%Y-%m-%d')
    end = datetime.date(int(end[0:4]), int(end[6:7]), int(end[8:10])) #DATA ATUAL
    info = list()
    while(start<end):
        info.append(str(start))
        start = start + datetime.timedelta(days=1)
    return info


def get_all_stations_cod(type: str):
    """
    Return list wtih all stations code according to its type.
    :param type: Only prec (precipitation) or flow
    """

    if type == 'flow':
        table = 'flowstations'
    elif type == 'prec':
        table = 'precstations'
    else:
        raise Exception("Type not supported")

    db = dbc.db_connection(host='localhost', user='root', password='root', database='eco')
    cursor = db.cursor()

    codes = dbc.select_row(cursor, 'codStation', 'eco', table)
    newlist = list() #for some reason, select return a list of tuples
    for cod in codes:
        inter = cod[0]
        newlist.append(inter)
    return newlist

def fillNulls(dates: list, codesStations: list, type: str):
    if type != 'flow' and type != 'prec':
        raise Exception("Type not supported")
    if codesStations == []:
        raise Exception("codesStations empty")
    if dates == []:
        raise Exception("dates empty")

    db = dbc.db_connection(host='localhost', user='root', password='root', database='eco')
    cursor = db.cursor()

    if type == 'flow':
        for codstation in codesStations:
            for date in dates:
                try:
                    dbc.insert_measuresflow_null(cursor, db, str(date), int(codstation))
                except Exception as e:
                    print(e)
    else:
        for codstation in codesStations:
            for date in dates:
                try:
                    dbc.insert_measuresprec_null(cursor, db, date, codstation)
                except Exception as e:
                    print(e)


codes = get_all_stations_cod('prec')
fillNulls(get_dates(), codes, type='prec')