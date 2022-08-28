from logging import exception
from queue import Empty
import requests
import datetime
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import sqlalchemy.exc
import psycopg2 as pg


#======= Set up variables ========================================================================================#
# Get token from https://developer.spotify.com/console/get-recently-played/
token = "BQCS9pGuHdLSRk3wYOereLCMIh5DAFxa-fRjNJG-ahuZMuZ5yKjMglv2BYbCplmVA7SAjyie98scZP-nr2Qvn_j7zgtSHCTElOi8yoIV5vKZTm7puAiRCuEpUQldvTMibAdm5a_U8VRRW1CLq0C8VQ2vHptci-e5SCbihaMAGFetzyc"
url = "https://api.spotify.com/v1/me/player/recently-played"

today = datetime.datetime.now()
yesterday = today - datetime.timedelta(days=21)
yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

db_dialect_api = 'postgresql+psycopg2'
db_username = 'postgres'
db_password = 'Superpass'
db_host = 'localhost'
db_port = '5432'
db_name = 'spotify_data'

engine_string = f'{db_dialect_api}://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'

# API required headers
headers = {
    "Accept" : "application/json",
    "Content-Type" : "application/json",
    "Authorization" : f"Bearer {token}"
}

# API parameters
params = {
    "limit" : 50, # must be <= 50
    "after" : yesterday_unix_timestamp,
    "before" : None
}

# Empty Lists for pushing data into
song_names = []
artist_names = []
played_at_list = []
timestamps = []
popularity = []
explicit = []

#======= Get Data from API ========================================================================================#

r = requests.get(url, headers = headers, params = params)
data = r.json()

# Save a formatted sample file to check what keys we want to save from the output
# import json
# pretty = json.dumps(data, indent = 4, sort_keys=True)
# file = open('raw_json.json', 'w')
# file.write(pretty)

#======= Validate the data ========================================================================================#

# Check if the data is good data or print the API error message.
try:
    api_error = data["error"]["message"]
except:
    api_error = 0
else:
    print(api_error)

# Function definition for after data is loaded into a dataframe
def isValid (df) -> bool:
    # check if dataframe is empty
    if df.empty:
        print('No songs loaded')
        return False
    
    # Check all primary keys are unique
    if df.played_at_list.is_unique:
        pass
    else:
        raise Exception('duplicate key loaded')
    
    # Check for null values
    if df.isnull().values.any():
        raise Exception('Null values loaded')


#======= Load Data into Data Frame ========================================================================================#

# If the data is good, load it
if api_error ==  0:

    
# Extracting only the relevant bits of data from the json object      
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        popularity.append(song["track"]["popularity"])
        explicit.append(song["track"]["explicit"])
       
    dict = {
        'song_names' : song_names,
        'artist_names' : artist_names,
        'played_at_list' : played_at_list,
        'popularity' : popularity,
        'explicit' : explicit
    }

    df = pd.DataFrame(dict)
    df.set_index('played_at_list')
    validation = isValid(df)
    if validation != None:
        print(validation)
        raise
    #print(df)

#======= load Data into SQL Database ========================================================================================#
engine = create_engine(engine_string)

# SQL query to create a temp table and its columns
table_create_string = """\
    CREATE TABLE
    played_songs_temp(
        song_names varchar(255),
        artist_names varchar(255),
        played_at_list timestamp PRIMARY KEY,
        popularity int,
        explicit boolean
    );
    """
            
table_merge_string = """\
    INSERT INTO played_songs
    SELECT * FROM played_songs_temp
    WHERE NOT EXISTS 
        (SELECT * 
        FROM played_songs 
        WHERE played_songs.played_at_list = played_songs_temp.played_at_list)
    """

drop_temp_table = """\
    DROP TABLE played_songs_temp

    """



# Wrapping database connection in a Try block to handle exceptions
try:
    with engine.begin() as conn:
        # create a temp table
        conn.execute(text(table_create_string))
        # add the dataframe to the temp table
        df.to_sql('played_songs_temp', con=conn, if_exists='append', index = False)
        # 
        conn.execute(text(table_merge_string))
        conn.execute(text(drop_temp_table))
    
        
except sqlalchemy.exc.IntegrityError:
    # Raised if trying to pass in rows with primary keys that already exist
    pass
