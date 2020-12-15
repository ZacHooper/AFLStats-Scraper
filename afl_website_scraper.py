'''
    Good examples
    - Regualar season (2012 round 1): https://www.afl.com.au/matches/1
    - Grand Final (2012): https://www.afl.com.au/matches/232
    - Pre Season Match (2020 Marsh): https://www.afl.com.au/matches/2022
    - Womens Grand Final (2017): https://www.afl.com.au/matches/1257
    - Womens Regular Season (2017 Round 2): https://www.afl.com.au/matches/1254
    - No data page: https://www.afl.com.au/matches/5
'''


from bs4 import BeautifulSoup
from tinydb import TinyDB, Query
from multiprocessing import Pool, Manager
import threading
import pandas as pd
import requests
import re
import sqlite3

def getDataForMatch(matchID):
    try:
        print("Attempting to fetch match: {0} with url: https://www.afl.com.au/matches/{0}".format(matchID))
        html_doc = requests.get("https://www.afl.com.au/matches/" + str(matchID))
        if html_doc.ok:
            
            soup = BeautifulSoup(html_doc.content, 'html.parser')

            # Condense Soup to more managaeble size
            match_info = soup.find(class_="mc-header__match-info")

            if match_info is None:
                return None

            # Pull out data from HTML
            competition = match_info.find(class_="mc-header__comp")
            round_info = match_info.find(class_="mc-header__round-wrapper")
            start_time = match_info.find(class_="mc-header__date-wrapper js-match-start-time")['data-start-time']
            stadium = match_info.find(class_="mc-header__venue-highlight")
            scores = match_info.find_all(class_="mc-header__score-main")
            splits = match_info.find_all(class_="mc-header__score-split")
            home_score = scores[0]
            home_split = splits[0]
            away_score = scores[1]
            away_split = splits[1]

            # Clean the data

            # see - regexr.com/58fd1 for better explanation about regex
            reg_season_round, finals_round, home_team, away_team = re.findall("(?:(?=^Round)Round\s(\w+)|((?:\w*|\w*\s)+))\s.\s(?!\sv\s)((?:\w*|\w*\s)+)\sv\s((?:\w*|\w*\s)+)$", round_info.string.strip())[0]

            home_goals, home_points = re.findall("^(\d*).(\d*)", home_split.string)[0]
            away_goals, away_points = re.findall("^(\d*).(\d*)", away_split.string)[0]

            season = re.search("^(\d{4})", start_time)[0]

            date, kick_off = re.findall("^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2})", start_time)[0]

            data = {
                "id": matchID,
                "competition": competition.string[5:],
                "season": season,
                "round": reg_season_round if reg_season_round != '' else finals_round,
                "date": date,
                "kick_off": kick_off,
                "stadium": stadium.string,
                "winner": home_team if int(home_score.string) > int(away_score.string) else away_team,
                "home_team": home_team,
                "home_score": int(home_score.string),
                "home_goals": int(home_goals),
                "home_points": int(home_points),
                "away_team": away_team,
                "away_score": int(away_score.string),
                "away_goals": int(away_goals),
                "away_points": int(away_points)
            }

            return data
        else:
            return None
    except:
        assert("Unable to fetch afl match: {0}".format(matchID))

def getAndInsertMatchIntoDB(db, matchID):
    match = getDataForMatch(matchID)
    if match is not None:
        with threading.Lock():
            print("inserting match: {0} where {1} won!".format(matchID, match['winner']))
            db.insert(match)
    else:
        print("Skipping Match with ID: {}".format(matchID))

def populateTestDB(matchID):
    db = TinyDB('test_db.json')
    getAndInsertMatchIntoDB(db, matchID)
    
def readTestDB():
    db = TinyDB('test_db.json')
    df = pd.DataFrame(db.all())
    print(df[df['away_points'] == df['away_points'].max()])

def main(matchID):
    match = getDataForMatch(matchID)
    if match is not None:
        try:
            sqliteConnection = sqlite3.connect('SQLite_Python.db')
            cursor = sqliteConnection.cursor()
            match = getDataForMatch(matchID)
            insert(cursor, match)
            rows = cursor.execute("SELECT * FROM matches").fetchall()
            cursor.close()
            sqliteConnection.commit()

        except sqlite3.Error as error:
            print("Error while connecting to sqlite", error)
        finally:
            if (sqliteConnection):
                sqliteConnection.close()
                print("The SQLite connection is closed")
    else:
        print("Skipping Match with ID: {}".format(matchID))

def createTable(cursor):
    cursor.execute('''CREATE TABLE matches (
        id INTEGER, 
        competition TEXT, 
        season TEXT,
        round TEXT,
        date TEXT,
        kick_off TEXT,
        stadium TEXT,
        winner TEXT,
        home_team TEXT,
        home_score INTEGER,
        home_goals INTEGER,
        home_points INTEGER,
        away_team TEXT,
        away_score INTEGER,
        away_goals INTEGER,
        away_points INTEGER
        )''')

def insert(cursor, match):
    print("Inserting Match: {0}".format(match['id']))
    cursor.execute('''INSERT INTO matches VALUES ({0}, "{1}", "{2}", "{3}", "{4}", "{5}", "{6}", "{7}", "{8}", {9}, {10}, {11}, "{12}", {13}, {14}, {15})'''.format(
            match['id'], 
            match['competition'],
            match['season'], 
            match['round'], 
            match['date'], 
            match['kick_off'], 
            match['stadium'], 
            match['winner'], 
            match['home_team'], 
            match['home_score'], 
            match['home_goals'], 
            match['home_points'], 
            match['away_team'], 
            match['away_score'], 
            match['away_goals'], 
            match['away_points']))

def openDB(db):
    try:
        sqliteConnection = sqlite3.connect(db)
        cursor = sqliteConnection.cursor()
        return (sqliteConnection, cursor)
    except sqlite3.Error as error:
        print("Error while connecting to sqlite", error)

def closeDB(connection, cursor):
    cursor.close()
    connection.close()

def fullUpdateDB(matchID):
    try:
        sqliteConnection = sqlite3.connect('SQLite_Python.db')
        cursor = sqliteConnection.cursor()
        rows = cursor.execute("SELECT * FROM matches WHERE id = " + str(matchID)).fetchall()
        if rows == []:
            match = getDataForMatch(matchID)
            if match is not None:
                insert(cursor, match)
                rows = cursor.execute("SELECT * FROM matches WHERE id = " + str(matchID)).fetchall()
            else:
                print("Skipping Match with ID: {}".format(matchID))
        sqliteConnection.commit()
        cursor.close()
        

    except sqlite3.Error as error:
        print("Error while connecting to sqlite", error)
    finally:
        if (sqliteConnection):
            sqliteConnection.close()
    

if __name__ == "__main__":
    import time
    start_time = time.time()

    with Pool(5) as pool:
        pool.map(fullUpdateDB, range(0,2815))

    print("--- %s seconds ---" % (time.time() - start_time))