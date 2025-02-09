import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker as sql_session
import psycopg2
import requests
import pytz
#-------------------------------------------------------CREDENTIALS---------------------------------------------------------------------------------
client_id = '_______________'
client_secret = '_____________'
refresh_token = "_____________"
aiven_db = '____________'
new_db = '____________'

#--------------------------------------------------------API_HELPER_FUNCTIONS-------------------------------------------------------------------

#_____1
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
accessToken = new_access_token(client_id, client_secret, refresh_token)

#_____2
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
k = get_recently_played(accessToken)['items']

#_____3
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

#---------------------------------------------INITIALIZING CONNECTION WITH EXISTING DATABASE(AVIEN)------------------------------------------------

# We'll pull all the existing data from there and quickly feed it to another database, and close this file, and update the database url in the automation script
aiven_conn = psycopg2.connect(aiven_db)
aiven_cursor = aiven_conn.cursor()

#Query to retrieve all the data
query_ms = "SELECT * FROM main_streaming_data LIMIT (SELECT COUNT(*) FROM main_streaming_data)-50;"

#Executing the query
aiven_cursor.execute(query_ms)

#Storing the result
records = aiven_cursor.fetchall()

#Converting result into pandas dataframe
main_df = pd.DataFrame(records, columns= [description[0] for description in aiven_cursor.description])

#Query to retrieve artist related data
query_artist = "SELECT * FROM artist_table;"

#Executing second query
aiven_cursor.execute(query_artist)

#Storing all the fetched data
records = aiven_cursor.fetchall()

#Converting to DataFrame
artist_df = pd.DataFrame(records, columns= [d[0] for d in aiven_cursor.description])

#Query to get track related data
query_track = "SELECT * FROM track_table;"

#Executing the last query
aiven_cursor.execute(query_track)

#Storing
records = aiven_cursor.fetchall()

#Converting to DataFrame
track_df = pd.DataFrame(records, columns= [d[0] for d in aiven_cursor.description])

#Terminating connection with old database
aiven_cursor.close()
aiven_conn.close()

#-----------------------------------INITIALIZING NEW CONNECTION WITH NEW DATABASE(NeonTech)ORM-------------------------------------------

#Creating engine
new_engine = create_engine(new_db, echo= True)

#Creating session maker
sesh = sql_session(bind= new_engine)
sesh = sesh()

#Defining a base class
Base = declarative_base()

#------------------------------------------------STRUCTURING OUR TABLES---------------------------------------------------------------------

#--------------------------------------------------MAIN_STREAMING_DATA----------------------------------------------------------------------
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

#-------------------------------------------------------ARITIST_TABLE--------------------------------------------------------------------------------
class artistTable(Base):
    __tablename__ = 'artist_table'
    artistId = Column('artist_id', String)
    artistName = Column('aritst_name', String)
    aritstPopularity = Column('popularity', Integer)
    genre = Column('genre', String)
    imgLink = Column('img_link', String)
    rowNum = Column('rn', Integer, primary_key= True, autoincrement= True)

    def __init__(self, artistId, artistName, artistPopularity, genre, imgLink):
        self.artistId = artistId
        self.artistName = artistName
        self.aritstPopularity = artistPopularity
        self.genre = genre
        self.imgLink = imgLink

    def __repr__(self):
        return f"({self.artistId}, {self.artistName}, {self.aritstPopularity}, {self.genre}, {self.imgLink})"

#------------------------------------------------------TRACK_TABLE-------------------------------------------------------------------------------
class trackTable(Base):
    __tablename__ = 'track_table'
    # defining columns
    trackID = Column('track_id', String)
    trackName = Column('track_name', String)
    explicit = Column('explicit', Boolean)
    popularity = Column('popularity', Integer)
    albumName = Column('album_name', String)
    imageLink = Column('img_link', String)
    featureArtists = Column('ft_artists', String)
    rowNum = Column('rn', Integer, primary_key= True, autoincrement= True)

    def __init__(self, trackID, trackName, explicit, popularity, albumName, imageLink, featureArtists):
        self.trackID = trackID
        self.trackName = trackName
        self.explicit = explicit
        self.popularity = popularity
        self.albumName = albumName
        self.imageLink = imageLink
        self.featureArtists = featureArtists
    
    def __repr__(self):
        return f"({self.trackID}, {self.trackName}, {self.explicit}, {self.popularity}, {self.albumName}, {self.imageLink}, {self.featureArtists})"

# Creates All the Tables with defined structure(only if not exists)
Base.metadata.create_all(new_engine)

#------------------------------------------------FEEDING DATA FROM DATAFRAMES-----------------------------------------------------------------

#_____1.MAIN_STREAMING_DATA
# for i, rows in main_df.iterrows():
#     data = streamingHistoryTable(rows['played_at'], rows['track_id'], rows['track_name'], rows['artist_name'], rows['aritst_id'], rows['duration_ms'])
#     sesh.add(data)
# sesh.commit()
# print("Done1")


#--Adding last 50 tracks
df_date, df_track_id, df_track_name, df_artist_id, df_artist_name, df_duration_ms = [], [], [], [], [], []

for i in k:
    df_date.append(processTime(i['played_at']))
    df_track_id.append(i['track']['id'])
    df_track_name.append(i['track']['name'])
    df_artist_id.append(i['track']['artists'][0]['id'])
    df_artist_name.append(i['track']['artists'][0]['name'])
    df_duration_ms.append(i['track']['duration_ms'])

streamingData = {
    'date':df_date,
    'track_id':df_track_id,
    'track_name':df_track_name,
    'artist_name':df_artist_name,
    'artist_id':df_artist_id,
    'duration':df_duration_ms
}

main_data = pd.DataFrame(streamingData)
main_data = main_data.iloc[::-1]

for i, rows in main_data.iterrows():
    data = streamingHistoryTable(rows['date'], rows['track_id'], rows['track_name'], rows['artist_name'], rows['artist_id'], rows['duration'])
    sesh.add(data)
sesh.commit()



#_____2.ARTIST_TABLE
# for i, rows in artist_df.iterrows():
#     data = artistTable(rows['artist_id'], rows['aritst_name'], rows['popularity'], rows['genre'], rows['img_link'])
#     sesh.add(data)
# sesh.commit()
# print("Done2")

#_____3.TRACK_TABLE
# for i, rows in track_df.iterrows():
#     data = trackTable(rows['track_id'], rows['track_name'], rows['explicit'], rows['popularity'], rows['album_name'], rows['img_link'], rows['ft_artists'])
#     sesh.add(data)
# sesh.commit()
# print("Done3")

sesh.close()
print('Done')


