import mysql.connector
def main():
    db = mysql.connector.connect(host='localhost', user='root', password='root')
    cursor = db.cursor()
    try:
        create_database(cursor, 'eco')
    except Exception as e:
        print(e)


    db = db_connection(host='localhost', user='root', password='root', database='eco')
    cursor = db.cursor()
    try:
        inicialize_tables(cursor=cursor)
    except Exception as e:
        print(e)


def db_connection(host: str, user: str, password: str, database: str):

    db = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    return db


def create_database(cursor: mysql.connector, database: str):
    command = 'CREATE DATABASE %s' % database
    cursor.execute(command)


def inicialize_tables(cursor: mysql.connector):
    """
    Creates designed tables, if not exists:
    SubBasin, FlowStations, MeasureFlow
    """
    try:
        create_table(
            cursor,
            'SubBasin',
            {'subBasinCod': 'INT',
             'name': 'TEXT NOT NULL',
            },
            other_data=['PRIMARY KEY (subBasinCod)']
        )
    except Exception as e:
        print(e)

    try:
        create_table(
            cursor,
            'FlowStations',
            {'codStation': 'INT',
             'name': 'TEXT NOT NULL',
             'subBasinCod': 'INT NOT NULL',
             'latitude': 'FLOAT NOT NULL',
             'longitude': 'FLOAT NOT NULL'
             }, other_data=['PRIMARY KEY (codStation)',
                           'FOREIGN KEY (subBasinCod) REFERENCES SubBasin(subBasinCod)']
        )
    except Exception as e:
        print(e)

    try:
        create_table(
            cursor,
            'MeasuresFlow',
            {'date': 'DATE NOT NULL',
             'codStation': 'INT',
             'flow': 'FLOAT'
            },
            other_data=['PRIMARY KEY (date, codStation)',
                       'FOREIGN KEY (codStation) REFERENCES FlowStations(codStation)']
        )
    except Exception as e:
        print(e)

    try:
        create_table(
            cursor,
            'PrecStations',
            {'codStation': 'INT',
             'name': 'TEXT NOT NULL',
             'subBasinCod': 'INT NOT NULL',
             'latitude': 'FLOAT NOT NULL',
             'longitude': 'FLOAT NOT NULL'
             }, other_data=['PRIMARY KEY (codStation)',
                           'FOREIGN KEY (subBasinCod) REFERENCES SubBasin(subBasinCod)']
        )
    except Exception as e:
        print(e)


    try:
        create_table(
            cursor,
            'MeasuresPrec',
            {'date': 'DATE NOT NULL',
             'codStation': 'INT NOT NULL',
             'precipitation': 'FLOAT'
            },
            other_data=['PRIMARY KEY (date, codStation)',
                        'FOREIGN KEY (codStation) REFERENCES PrecStations(codStation)']
        )
    except Exception as e:
        print(e)

def create_table(cursor: mysql.connector, table: str, fields: dict, other_data: list):

    command = "CREATE TABLE %s (%s)" % (
        table,
        ','.join([k + ' ' + v for k, v in fields.items()] + (
            other_data if other_data is not None else []))
    )
    cursor.execute(command)

def insert_measuresflow(cursor: mysql.connector,db, date: str, codstation: str, flow: str):
    command = 'INSERT INTO eco.measuresflow(date, codstation, flow) VALUES(%s, %s, %s)'
    values = (date, codstation, flow)
    cursor.execute(command, values)
    db.commit()

def insert_measuresprec(cursor: mysql.connector,db, date: str, codstation: str, precipitation: str):
    command = 'INSERT INTO eco.measuresprec(date, codstation, precipitation) VALUES(%s, %s, %s)'
    values = (date, codstation, precipitation)
    cursor.execute(command, values)
    db.commit()

def insert_measuresflow_null(cursor: mysql.connector,db, date: str, codstation: str):
    command = 'INSERT INTO eco.measuresflow(date, codstation) VALUES(%s, %s)'
    values = (date, codstation)
    cursor.execute(command, values)
    db.commit()

def insert_measuresprec_null(cursor: mysql.connector,db, date: str, codstation: str):
    command = 'INSERT INTO eco.measuresprec(date, codstation) VALUES(%s, %s)'
    values = (date, codstation)
    cursor.execute(command, values)
    db.commit()


def insert_precstations(cursor: mysql.connector, db, codStation: str, name: str, subBasinCod: str, latitude: str, longitude: str):
    command = 'INSERT INTO eco.precstations(codStation, name, subBasinCod, latitude, longitude) VALUES(%s, %s, %s, %s, %s)'
    values = (codStation, name, subBasinCod, latitude, longitude)
    cursor.execute(command, values)
    db.commit()

def insert_flowstations(cursor: mysql.connector, db, codStation: str, name: str, subBasinCod: str, latitude: str, longitude: str):
    command = 'INSERT INTO eco.flowstations(codStation, name, subBasinCod, latitude, longitude) VALUES(%s, %s, %s, %s, %s)'
    values = (codStation, name, subBasinCod, latitude, longitude)
    cursor.execute(command, values)
    db.commit()


def select_row(cursor: mysql.connector, column: str, database: str,table: str):
    command = 'SELECT %s FROM %s.%s' % (column, database, table)
    cursor.execute(command)
    rows = cursor.fetchall()

    return rows


if __name__ == '__main__':
    main()