import pandas as pd
import base64
import requests
import os
import json
from datetime import datetime
import pytz
from sqlalchemy import create_engine, Column, Integer, String,  Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker 
import psycopg2


#----------------------------------------------------------LOADING CREDENTIALS--------------------------------------------------------------------

# this is the new client id and client secret, in this script we'll not use O-Auth for API calls, instead we'll follow Client Credentials Flow
client_id = os.environ['CC_CLIENT_ID']
client_secret = os.environ['CC_CLIENT_SECRET']

#-------------------------------------------------------GENERATING TOKEN----------------------------------------------------------------------

def getToken():
    authString = client_id + ':' + client_secret
    #encoding it to utf-8
    authString = authString.encode('utf-8')
    authString = str(base64.b64encode(authString), 'utf-8')
    #endpoint
    url = 'https://accounts.spotify.com/api/token'
    header = {
        'Authorization': 'Basic ' + authString,
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {'grant_type': 'client_credentials'}

    response = requests.post(url, headers= header, data= data)
    response = json.loads(response.content)

    return response['access_token']

v = getToken()

#------------------------------------------------------GENERATING AUTHENTICATION HEADER-------------------------------------------------------
def getAuthHeader(token):
    return {'Authorization':'Bearer ' + token}


#------------------------------------------------------TRACKS STATS(DATA RELATED TO TRACKS)--------------------------------------------------
def getTracks(token, trackId):
    url = f"https://api.spotify.com/v1/tracks/{trackId}"
    header = getAuthHeader(token)
    response = requests.get(url, headers= header)
    response = json.loads(response.content)
    return response

#-------------------------------------------------------ARTIST STATS (DATA RELATED TO ARTISTS)-------------------------------------------------

def getArtist(token, artistId):
    endpoint = f"https://api.spotify.com/v1/artists/{artistId}"
    header = getAuthHeader(token)
    response = requests.get(endpoint, headers=header)
    response = json.loads(response.content)
    return response

#------------------------------------------------DEFINING CONNECTION WITH POSTGRES DATABASE (ORM)--------------------------------------------------
aivenURL = os.environ['AIVEN_URL']
# Creating Engine
db_engine = create_engine(aivenURL, echo= True) #this takes care of sql conversion of python commands

# Making a new session for interacting with database
Session = sessionmaker(bind= db_engine)
sql_session_1 = Session()
sql_session_2 = Session()
# Defining a base class
Base = declarative_base()
#------------------------------------------------------CREATING ARTIST TABLE--------------------------------------------------------------------
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
    
#------------------------------------------------------CREATING ARTIST TABLE-----------------------------------------------------------------------
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
    
Base.metadata.create_all(db_engine)

# Since now we've created our data the next thing would be to run queries in the data set because on the basis of these queries we'll populate our tables
# So firstly we'll create a psycogp2 connection

#-----------------------------------------------------PSYCOGP2 CONNECTION-----------------------------------------------------------------
conn = psycopg2.connect(aivenURL)
cursor = conn.cursor()

# query
cursor.execute("""
    SELECT DISTINCT(track_id)
    FROM main_streaming_data
    WHERE track_id NOT IN (SELECT track_id FROM track_table)
""")

track_ids = cursor.fetchall()

# query
cursor.execute("""
    SELECT DISTINCT(aritst_id)
    FROM main_streaming_data
    WHERE aritst_id NOT IN (SELECT artist_id FROM artist_table)
""")

artist_ids = cursor.fetchall()

artistId, trackId = [], []
for i in track_ids:
    trackId.append(i[0])
for i in artist_ids:
    artistId.append(i[0])

cursor.close()
conn.close()

#-----------------------------------------------------FETCH AND POPULATE ARTIST DATA----------------------------------------------------------------------
if len(artistId) > 0:
    artist_id, artist_name, artist_popularity, genre, img_link = [], [], [], [], []
    for id in artistId:
        artistStats = getArtist(v, id)
        artist_id.append(id)
        artist_name.append(artistStats['name'])
        artist_popularity.append(artistStats['popularity'])
        if artistStats['genres']:
            genre.append(artistStats['genres'][0])
        else:
            genre.append("NaN")
        if artistStats['images'][0]['url']:
            img_link.append(artistStats['images'][0]['url'])
        else:
            img_link.append('')
    data  = {
        'artist_id':artist_id,
        'artist_name':artist_name,
        'artist_popularity':artist_popularity,
        'genre':genre,
        'img_link':img_link
    }
    artist_df = pd.DataFrame(data)

    for i, rows in artist_df.iterrows():
        artist_data  = artistTable(rows['artist_id'], rows['artist_name'], rows['artist_popularity'], rows['genre'], rows['img_link'])
        sql_session_1.add(artist_data)

    sql_session_1.commit()
    sql_session_1.close()
else:
    sql_session_1.close()
#------------------------------------------------FETCH AND POPULATE TRACKS DATA--------------------------------------------------------------
if len(trackId) > 0:
    track_id, track_name, explicit, popularity, albumName, imageLink, featureArtists = [], [], [], [], [], [], []
    for i in trackId:
        trackStats = getTracks(v, i)
        track_id.append(i)
        track_name.append(trackStats['name'])
        explicit.append(trackStats['explicit'])
        popularity.append(trackStats['popularity'])

        if trackStats['album']['total_tracks'] > 3:
            albumName.append(trackStats['album']['name'])
        else:
            albumName.append('EP/SINGLE')
        imageLink.append(trackStats['album']['images'][0]['url'])
        allArtists = ''
        if len(trackStats['artists']) > 1:
            artists = trackStats['artists']
            for i in range(1, len(artists)):
                if i == len(artists)-1:
                    allArtists += artists[i]['name']
                else:
                    allArtists += artists[i]['name']+', '
            featureArtists.append(allArtists)
        else:
            featureArtists.append('NaN')
    data = {
        'track_id':track_id,
        'track_name':track_name,
        'explicit':explicit,
        'popularity':popularity,
        'album_name':albumName,
        'image_link':imageLink,
        'ft_artists':featureArtists
    }

    tracks_df = pd.DataFrame(data)
    
    for i, rows in tracks_df.iterrows():
        tracksData = trackTable(rows['track_id'], rows['track_name'], rows['explicit'], rows['popularity'], rows['album_name'], rows['image_link'], rows['ft_artists'])
        sql_session_2.add(tracksData)
    sql_session_2.commit()
    sql_session_2.close()
else:
    sql_session_2.close()


