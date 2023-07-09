import mysql.connector
import pandas as pd
import glob
import os

TABLES = ['ejobs', 'emag_tel', 'flip', 'olx_auto', 'olx_moto']
CONNECTION = mysql.connector.connect(
    host="localhost",
    user="root",
    password="laleaua",
    database="db_webscrape"
)


def create_table():
    mycursor = CONNECTION.cursor()
    list_of_dirs = glob.glob("D:/P/Webscrapers/BD/*")
    for i, j in zip(list_of_dirs, TABLES):
        list_of_files = glob.glob(f"{i}/*.csv")
        if not list_of_files:
            pass
        else:
            latest_file = max(list_of_files, key=os.path.getctime)
            data = pd.read_csv(latest_file)

            create_table_query = f"CREATE TABLE {j} ("
            create_table_query += f"{data.columns[0]} INT PRIMARY KEY,"
            for col in data.columns[1:]:
                create_table_query += f"{col} VARCHAR(255),"
            create_table_query = create_table_query.rstrip(',') + ")"

            mycursor.execute(create_table_query)

    mycursor.close()
    CONNECTION.close()


def write_in_table():
    mycursor = CONNECTION.cursor()
    list_of_dirs = glob.glob("D:/P/Webscrapers/BD/*")
    for i, j in zip(list_of_dirs, TABLES):
        list_of_files = glob.glob(f"{i}/*.csv")
        latest_file = max(list_of_files, key=os.path.getctime)
        data = pd.read_csv(latest_file)

        data.fillna('', inplace=True)

        for _, row in data.iterrows():
            vals = tuple(row)
            mycursor.execute(f"INSERT INTO {j} VALUES {vals}")
    mycursor.close()
    CONNECTION.commit()


def empty_table():
    mycursor = CONNECTION.cursor()
    for table in TABLES:
        mycursor.execute(f"DROP TABLE {table}")
        CONNECTION.commit()


if __name__ == "__main__":
    # create_table()
    write_in_table()
    # empty_table()
