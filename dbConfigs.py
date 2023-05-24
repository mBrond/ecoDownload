import mysql.connector

def main ():
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="eco"
    )

    cursor = db.cursor()

    try:
        createTable(
            cursor,
            'SubBasin',
            {'subBasinCod':'INT',
             'name':'TEXT NOT NULL',
            },
            other_data=['PRIMARY KEY (subBasinCod)']
        )
    except Exception as e:
        print(e)

    try:
        createTable(
            cursor,
            'FlowStations',
            {'codStation': 'INT',
             'name': 'TEXT NOT NULL',
             'subBasinCod': 'INT NOT NULL',
             'consistencyLvl':'INT NOT NULL'
             }, other_data=['PRIMARY KEY (codStation)',
                           'FOREIGN KEY (subBasinCod) REFERENCES SubBasin(subBasinCod)']
        )
    except Exception as e:
        print(e)

    try:
        createTable(
            cursor,
            'MeasuresFlow',
            {'date':'DATE',
             'codStation':'INT',
             'flow': 'DOUBLE'
            },
            other_data=['PRIMARY KEY (date, codStation)',
                       'FOREIGN KEY (codStation) REFERENCES FlowStations(codStation)']
        )
    except Exception as e:
        print(e)


def createTable(cursor: mysql.connector, table: str, fields: dict, other_data: list):

    command = "CREATE TABLE %s (%s)" % (
        table,
        ','.join([k + ' ' + v for k, v in fields.items()] + (
            other_data if other_data is not None else []))
    )
    cursor.execute(command)

def insertRow(cursor: mysql.connector, table: str, tuples: list):
    pass

def selectRow(cursor: mysql.connector):
    pass

if __name__ == '__main__':
    main()