import pandas as pd
import requests
import os
from datetime import datetime
import pytz
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker 
import psycopg2

#------------------------------------------------------------LOADING CREDENTIALS--------------------------------------------------------------

client_id = os.environ['CLIENT_ID']
client_secret = os.environ['CLIENT_SECRET']
refreshToken = os.environ['REFRESH_TOKEN']

#------------------------------------------------------------READING JSON TO GET TOKENS------------------------------------------------------
# def load_tokens():
#     with open('tokens.json', 'r') as f:
#         return json.load(f)

# accessToken, refreshToken = load_tokens()['access_token'], load_tokens()['refresh_token']

# ---------------------------------------------------CREATING NEW ACCESS TOKEN----------------------------------------------------------------
# getting new access token with help of refresh token
def new_access_token(client_id, client_secret, refresh_token):
    """Use the refresh token to get a new access token."""
    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret
    }
    response = requests.post(url, headers=headers, data=data)
    response = response.json()
    return response.get('access_token')
accessToken = new_access_token(client_id, client_secret, refreshToken) # we'll never run in 401 error now 

# --------------------------------------------------------GET RECENTLY PLAYED TRACKS--------------------------------------------------------

def get_recently_played(access_token):
    """Fetch the last 20 played tracks using the given access token."""
    url = "https://api.spotify.com/v1/me/player/recently-played?limit=50"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    
    response = requests.get(url, headers=headers)
    print(response.status_code)

    if response.status_code == 200:
        return response.json()  # Return the JSON response
    else:
        print(f"Error: {response.status_code} - {response.json()}")
        return None

#------------------------------------------------------RETRIEVING THE LAST 50 TRACKS---------------------------------------------------------
k = get_recently_played(accessToken)['items']

# for i in k:
#     track = i['track']['name']
#     played_for = i['played_at']
#     print(track, played_for)

# as we can see that we've time as string data type in UTC format before fetching this into our database, we'll need to process this into IST

#------------------------------------------------------------CHANGING STRING INTO DATETIME-----------------------------------------------------

def processTime(timestamp_str):
    # Parse the input timestamp (UTC)
    utc_dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    # Define UTC and IST time zones
    utc_zone = pytz.utc
    ist_zone = pytz.timezone('Asia/Kolkata')
    # Convert to UTC first (though it's already UTC)
    utc_dt = utc_zone.localize(utc_dt)
    # Convert to IST
    ist_dt = utc_dt.astimezone(ist_zone)
    return ist_dt  # Returning datetime object, not string

# let's check our function quickly

# for i in k:
#     track = i['track']['name']
#     played_for = processTime(i['played_at'])
#     print(track, played_for)

# now let's draw our requirements regarding things we want to store in our main streaming history data:
# date, trackID, trackName, artitstId, artistName, total_duration
# we'll not process the time here, as this will increasing our overall JOB time, or again will increase the overall size in cloud
# we can process the time in our local BI tool where we'll import this data from cloud

# let's firstly try to draw a successful dataframe as mentioned above after that we'll define table using SQLALCHEMY's ORM

# df_date, df_track_id, df_track_name, df_artist_id, df_artist_name, df_duration_ms = [], [], [], [], [], []

# for i in k:
#     df_date.append(processTime(i['played_at']))
#     df_track_id.append(i['track']['id'])
#     df_track_name.append(i['track']['name'])
#     df_artist_id.append(i['track']['artists'][0]['id'])
#     df_artist_name.append(i['track']['artists'][0]['name'])
#     df_duration_ms.append(i['track']['duration_ms'])

# streamingData = {
#     'date':df_date,
#     'track_id':df_track_id,
#     'track_name':df_track_name,
#     'artist_name':df_artist_name,
#     'artist_id':df_artist_id,
#     'duration':df_duration_ms
# }

# main_data = pd.DataFrame(streamingData)

# reversing the dataframe so that we can add this into tables in an incremental time form, thus making data more readable and optimized querying
# main_data = main_data.iloc[::-1]


# this is working just fine as we expected now we need a database hosted in cloud in which we'll store all of this data
# we'll host different data into different tables as of now, I'll create two table in this database let's just quickly set up a postgres table in cloud
# this time I've hosted my postgres database on aiven.io
# let's quickly setup our first table with the same structure as our dataframe here
# and then we'll use some sort of logic through which we'll make sure the entry that we'll make to database is the latest ones and not duplicates
# because one thing is clear over here that we've given data as latest first and old later and we don't want to store it in the same way
# so a reverse loop would be an easy option for that but for or later entries we'll compare the times of both the one we've converted over here and the one in table
# because time will always increase, we'll feed only that specific data that will be after the last feeded data

# ----------------------------------------------------------SETTING UP DATABASE (ORM)----------------------------------------------------------------------
aivenURL = os.environ['AIVEN_URL']

# CREATING ENGINE FOR CONNECTION USING DATABASE URL
aivenEngine = create_engine(aivenURL, echo= True)

# CREATING A SESSION MAKER FOR INTERACTION WITH DATABASE
SessionLocal = sessionmaker(bind= aivenEngine)
session_sql = SessionLocal()

# DEFINING A BASE CLASS
Base = declarative_base()
# ----------------------------------------------------------TABLE(STREAMING DATA)----------------------------------------------------------------------

class streamingHistoryTable(Base):
    __tablename__ = 'main_streaming_data'
    # defining columns
    date = Column('played_at', DateTime(timezone= True))
    track_id = Column('track_id', String)
    track_name = Column('track_name', String)
    artist_name = Column('artist_name', String)
    aritst_id = Column('aritst_id', String)
    duration_ms = Column('duration_ms', Integer)
    rowNum = Column('rowNum', Integer, primary_key= True, autoincrement= True)

    def __init__(self, date, track_id, track_name, artist_name, artist_id, duration_ms):
        self.date = date
        self.track_id = track_id
        self.track_name = track_name
        self.artist_name = artist_name
        self.aritst_id = artist_id
        self.duration_ms = duration_ms

    def __repr__(self):
        return f"({self.date}, {self.track_id}, {self.track_name}, {self.artist_name}, {self.aritst_id}, {self.duration_ms})"
    
# Creates All the Tables with defined structure(only if not exists)
Base.metadata.create_all(aivenEngine)


# feeding my created table its first ever data pool that contains last heard 50 tracks and there information which we've fetched from API and stored in our DataFrame

# for i, rows in main_data.iterrows():
#     data = streamingHistoryTable(rows['date'], rows['track_id'], rows['track_name'], rows['artist_name'], rows['artist_id'], rows['duration'])
#     session_sql.add(data)

# committing the whole insertion process
# session_sql.commit()

# closing the session
# session_sql.close()

# now let's find the last datetime from the column using this, for that we'll use psycogp2

# -------------------------------------------------------------PSYCOGP2 CONNECTION----------------------------------------------------------
conn = psycopg2.connect(aivenURL)
cursor = conn.cursor()

# query
cursor.execute("""
    SELECT MAX(played_at)
    FROM main_streaming_data
""")

res = cursor.fetchone()

# now it's done clear that we can compare the date from existing data and on the basis of it we can also create a dataframe now let's quickly create on the basis of comparison

df_date, df_track_id, df_track_name, df_artist_id, df_artist_name, df_duration_ms = [], [], [], [], [], []

addData = 0
for i in k:
    played_at = processTime(i['played_at'])
    if played_at > res[0]:
        addData = 1
        df_date.append(played_at)
        df_track_id.append(i['track']['id'])
        df_track_name.append(i['track']['name'])
        df_artist_id.append(i['track']['artists'][0]['id'])
        df_artist_name.append(i['track']['artists'][0]['name'])
        df_duration_ms.append(i['track']['duration_ms'])

if addData == 1:
    streamingData = {
        'date':df_date,
        'track_id':df_track_id,
        'track_name':df_track_name,
        'artist_name':df_artist_name,
        'artist_id':df_artist_id,
        'duration':df_duration_ms
    }
    main_data = pd.DataFrame(streamingData)
    # reversing
    main_data = main_data.iloc[::-1]

    # feeding data to table using ORM
    for i, rows in main_data.iterrows():
        data = streamingHistoryTable(rows['date'], rows['track_id'], rows['track_name'], rows['artist_name'], rows['artist_id'], rows['duration'])
        session_sql.add(data)
    
    session_sql.commit()
    session_sql.close()
else:
    session_sql.close()
    

cursor.close()
conn.close()

# I've verified the fact that with one refresh token we can refresh our access token indefinitely now all we need is to store these two
# We'll hide our refresh token in our environment and then each time we'll need to make a call we'll just create a new access token to work with
# This will insure a free flow of code each time and we'll save us the hassle of storing access tokens and reading and updating them 
# As of now, I'll comment out the code in which I'm retrieving my tokens from JSON, and read directly from .env
