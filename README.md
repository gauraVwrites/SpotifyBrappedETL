# SpotifyBrappedETL
## Spotify Wrapped But Better
#
**Purpose**
- Create dynamic insights for free using my ETL skills, which in Spotify otherwise requires a Premium subscription.
- To strengthen my ETL skills by working with multiple tables.
#
**Dataset**
- Created my own database from scratch, database consists of 3 tables.
- mainStreamingData | artistTable | trackTable
- artistTable and trackTable act as Dimension Table while mainStreamingData acts as Fact Table.
- mainStreamingData contains information related to last played tracks like trackname, artistname, ids, timestamp.
#
### **Methodology**
#
**Languages,Libraries and Tools Used**
- Python
- requests(for making API calls)
- sqlalchemy(for creating database connection)
- psycopg2(for creating a database connection)
- postgresql(for database)
- github Actions(for Automation)
- NeonTech(for hosting my postgres table on cloud)
- Power BI(for Dashboarding and Reporting)
#
**Workflow**
#
![](https://github.com/gauraVwrites/SpotifyBrappedETL/blob/main/images/Screenshot%202025-02-18%20124533.png)<br>
#
Tasks that were done before starting the automated ETL pipeline:
- Hosted a database on cloud.
- Setting up O-Auth API.
- Made test API calls to make meaning of data that we need.
- Understood and converted the timezone of API data to IST.
- Created and Populated my fact table with first entry.
- Created Dimension tables.
- Wrote a script to populate fact table by querying the fact table and understand last updated date, and then use this date to take a decision to populate the table or not.
- In the same way wrote a script that queries both Fact and Dimension Tables and takes decision of populating Dimension Tables on the basis of result of queries.
#
1. *Understanding the API*<br>
#
In my last Spotify project Brapped, I've mentioned in one of my articles about two ways of interacting with Spotify's Web API. Client Cred<entials Flow | O - Authorization or Authorization Code Flow. <br>
Last time I used Client Credentials Flow. Since this time I need to work with data related to my Spotify Account I need to work with Authorization Code Flow. Authoriztion Code Flow requires user permission in order to make API calls that involve data related to user. It starts with generating an Authorization Code, to get Authorization Code we need to grant access from our Spotify Account, this gives us an one time Authorization Code, that helps us generate an Access Token which works exactly for 1 hour. Now in order to automate the whole flow, I'll use Refresh Token. A Refresh Token allows us to generate new Access Tokens, as many times as user wants.<br>
After sorting out my Access Token, I'll use 3 endpoints that will help me fetch the required data.<br>
This endpoint will be used in order to fetch data related to last played tracks https://api.spotify.com/v1/me/player/recently-played.<br>
These endpoints will be used to fetch data related to dimensions https://api.spotify.com/v1/tracks/{id} | https://api.spotify.com/v1/artists/{id}<br>
2. *Setting up the Cloud Environment*<br>
#
![]()
#
3. *Creating Tables*<br>
#
I've setup a 3 table database, mainStreamingData acts as a fact table, it records the information related to tracks played by user(me). Other two tables act as dimension table which store information about artists and tracks. These 3 tables get populated with the tracks I play from my spotify account.<br>
4. *Code Workflow*<br>
#
The code in ***getLast50.py*** starts with creating an access token using the existing refresh token, after that the token is created it quickly sends and get request to Spotify Web API, to fetch last played 50 tracks. After that all of the data is converted into a dataframe. This is followed with establishing a database connection with database using sqlalchemy and psycopg2, after that a query is sent to fact table to get the last entries timestamp which is then compared with the timestamp in dataframe and only the data that has a timestamp value more than the queried timestamp is populated back to fact table.<br>
The code in ***artistandTracks.py*** used Client Credentials Flow, in order to make API calls, this starts with generating Access Token using Client Credentials. Followed up with establishing connection with database and then querying fact table and dimension table and populating dimension table on the basis of queried results.<br>
5. *Automation*<br>
#
For timely running my python scripts I've used GitHub Actions.<br>
***getLast50.py*** runs every hour.<br>
***artistAndTracks.py*** runs every 3 hours.<br>
#
**My Added Features and Functionalities**
- Feature of daily report, shows my spotify report for current day.
- User interaction to search for user and tracks and get information related to them.
- Calculating duration ms on the way for each track.
- Distincting the artist stats on the basis of Ft. and Main.
#
**Challenges**<br>
- Limited information for more accurate insights, like no data on duration of track played.
- Normally Authorization Code Flow requires user to generate new Auth Code each time the code runs, making it hard to automate the whole process.
- Migrating the database from Aiven cloud to NeonTech.
- Making sure that only the fresh data is being populated into tables.
#
**Solutions**<br>
- Calculated duration on the go for more accurate insights related to duration.
- Carefully extracted and used Refresh Token and used it repeatedly to generate fresh Access Token, making the whole process automation friendly.
- Refer to ***migrate.py*** to understand the whole data migration flow.
- Populated data only after understanding the existing data in the tables by querying the database.
#
Thanks for sticking to this project, if you have any question related to this project, feel free to ask me. If you've any suggestion that will help me improve this project please let me know. Thanks again 😇


