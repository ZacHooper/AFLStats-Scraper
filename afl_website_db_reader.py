import sqlite3
import pandas as pd

def openDataframe():
    try:
        sqliteConnection = sqlite3.connect('SQLite_Python.db')
        df = pd.read_sql_query("SELECT * FROM matches", sqliteConnection)
        return df

    except sqlite3.Error as error:
        print("Error while connecting to sqlite", error)
    finally:
        if (sqliteConnection):
            sqliteConnection.close()
            print("The SQLite connection is closed")

def main():
    df = openDataframe()
    print(df.info(verbose=True))
    

if __name__ == "__main__":
    import time
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
    