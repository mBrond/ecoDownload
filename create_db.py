import mysql.connector

def main ():
    db = mysql.connector.connect(
        host="localhost",
        user="Brondani",
        password="admin"
    )

    cursor = db.cursor()

    cursor.execute("CREATE DATABASE database")

def createTable(cursor: mysql.Cursor, table: str, fields: dict):
    # cursor.execute('''
    #     CREATE TABLE IF NOT EXISTS
    #
    #
    #
    # ''')
    pass

def insertRow(cursor: mysql.Cursor, table: str, tuples: list):
    pass

def selectRow(cursor: mysql.Cursor, )

if __name__ == '__main__':
    main()