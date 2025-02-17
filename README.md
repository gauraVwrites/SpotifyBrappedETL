# SpotifyBrappedETL
## Spotify Wrapped But Better
#
**Purpose**
- Create dynamic insights for free using my ETL skills, which in Spotify otherwise requires a Premium subscription.
- To strengthen my ETL skills by working with multiple tables.
#
**Dataset**
- Created my own database from scratch, database consists of 3 tables.
- mainStreamingData|artistTable|trackTable
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
![]()<br>
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
In my last Spotify project Brapped, I've mentioned in one of my articles about two ways of interacting with Spotify's Web API. Client Credentials Flow | O - Authorization or Authorization Code Flow. <br>
Last time I used Client Credentials Flow. Since this time I need to work with data related to my Spotify Account I need to work with Authorization Code Flow. Authoriztion Code Flow requires user permission in order to make API calls that involve data related to user. It starts with generating an Authorization Code, to get Authorization Code we need to grant access from our Spotify Account, this gives us an one time Authorization Code, that helps us generate an Access Token which works exactly for 1 hour. Now in order to automate the whole flow, I'll use Refresh Token. A Refresh Token allows us to generate new Access Tokens, as many times as user wants.<br>
After sorting out my Access Token, I'll use 3 endpoints that will help me fetch the required data.
