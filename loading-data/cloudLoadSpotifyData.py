import json
import requests
import Dynamic_SpotifyOAuth
import connGCPDB
import time
from dateutil import tz
from datetime import datetime
import sqlalchemy


def assignTmChrt(utcTime): #this function uses the time that the songs were listened to, to assign a time of day classification
    try: #format 
        utc = datetime.strptime(str(utcTime), "%Y-%m-%dT%H:%M:%S.%fZ") 
    except:
        # ("Already Formatted")
        utc = datetime.strptime(str(utcTime), "%Y-%m-%dT%H:%M:%SZ")
        
    nUTC = utc.replace(tzinfo=tz.gettz("UTC"))

    locTime = nUTC.astimezone(tz.gettz("America/Chicago")) #changes from UTC time to local time 

    strLocTime = str(locTime)

    numTime = int(strLocTime[11:13] + strLocTime[14:16]) #splits the time string and concatenates to create form HHMM. Example: 1235

    # classification based on the time
    if ((numTime) >= 500) and ((numTime) < 1000):
        # "Early-morning"
        timeOfDay = 0

    elif ((numTime) >= 1000) and ((numTime) < 1200):
        # "Mid-Morning"
        timeOfDay = 1

    elif ((numTime) >= 1200) and ((numTime) < 1400):
        # "Early-Afternoon"
        timeOfDay = 2

    elif ((numTime) >= 1400) and ((numTime) < 1900):
        # "Afternoon"
        timeOfDay = 3

    elif ((numTime) >= 1900) and ((numTime) < 2100):
        # "Evening"
        timeOfDay = 4

    elif ((numTime) >= 2100) and ((numTime) < 2359):
        # "Night"
        timeOfDay = 5

    else:
        # "Late-Night"
        timeOfDay = 6


    # print(timeOfDay)

    return timeOfDay

def getGenre(id): # Gets the list of genres associated with the artist
    artURL = "https://api.spotify.com/v1/artists/" + id
    gHeader = {"Authorization": f"Bearer {OauthToken}"}
    dStream = requests.get(artURL, headers=gHeader).text
    gData = json.loads(dStream)
    return (gData["genres"])

def classifyGenre(g):  # returns a list containing Genre and subGenre, respectively
    # simplified version of what I plan to do
    mainG = ["jazz", "hip hop", "country", "rock", "blues", "classical music", "pop", "folk", "r&b", "soul", "metal", "reggae", "alternative rock", "indie rock", "punk", "underground hip hop", "pop", "punk", "techno", "electronic", "house", "funk", "rap"]

    # don't go through if the list only has one thing in it
    # anything that passes this has at least 2 items and will not throw an error;
    if len(g) < 2:
        return g

    gList = list()

    for item in g:
        if item in mainG:
            gList.append(item)
            g.remove(item)
            break

    if len(gList) == 0:
        gList.append(g[0])
        gList.append(g[1])
    else:
        gList.append(g[0])

    return gList

def loadArtistData(name, genList, spotArtID):
    genreIDS = list()
# for artist in item["track"]["artists"]:
#     goop[artist["name"]] = goop.get(artist["name"], 0) + 1
    if len(genList) != 0:

        for genre in genList:
            genreID = cur.execute(sqlalchemy.text("SELECT id FROM Genres where name=(:name)"), parameters= {"name": genre,})
            nID = genreID.fetchone()
            if type(nID) != tuple:
            # print("Not in Genres")
                cur.execute(sqlalchemy.text("INSERT IGNORE INTO Genres (name) VALUES (:name)"), parameters= {"name": genre,})
                cur.commit()
                nID = cur.execute(sqlalchemy.text("SELECT id FROM Genres where name=(:name)"), parameters= {"name": genre,})
            # print(nID)
            time.sleep(.001)
            genreIDS.append(nID.fetchone()[0])
    else:
        cur.execute(sqlalchemy.text("INSERT IGNORE INTO Artists (name, spotifyArtistID) VALUES (:name, :spotID)"), parameters = { "name" : name, "spotID" : spotArtID,})
        cur.commit()

    if (len(genreIDS) == 1):
        cur.execute(sqlalchemy.text("INSERT IGNORE INTO Artists (name, genre_id, spotifyArtistID) VALUES (:name, :genID, :spotID)"), parameters={"name" : name, "genID" : genreIDS[0], "spotID" : spotArtID,})
    elif (len(genreIDS)) == 2:
        # print("done")
        cur.execute(sqlalchemy.text("INSERT IGNORE INTO Artists (name, genre_id, subgenre_id, spotifyArtistID) VALUES (:name, :genID, :subGen, :spotID)"), parameters={"name" : name, "genID" : genreIDS[0], "subGen" : genreIDS[1], "spotID" : spotArtID,})
    genreIDS.clear()
    cur.commit()


def loadTrackData(tName, spotAlID, alName, spotifyTrackID, timePlayed, tmChart):

    albumID = cur.execute(sqlalchemy.text("SELECT id FROM Albums where name= (:name)"), parameters={"name": alName,})
    alID = albumID.fetchone()
    if type(alID) == None: # if not in the database
        print("Not in Albums")
        cur.execute(sqlalchemy.text("INSERT IGNORE INTO Albums (name, spotifyAlbumID) VALUES (:name, :alID)"), parameters={"name" : alName, "alID" : spotAlID,})
        cur.commit()
        albumID = cur.execute(sqlalchemy.text("SELECT id FROM Albums where name= (:name)"), parameters= {"name" : alName,})
        time.sleep(.0001)
        alID = albumID.fetchone()

    # print(albumID.fetchone()[0])

    cur.execute(sqlalchemy.text("INSERT IGNORE INTO Tracks (name, album_id, spotifyTrackID, playedAt, timeChart) VALUES (:name, :alID, :tID, :playedAt, :timeChart)"), parameters={"name": tName, "alID" : alID[0], "tID" : spotifyTrackID, "playedAt" : timePlayed, "timeChart" : tmChart})
    cur.commit()

# print("Not in Albums")

try: 
    OauthToken = Dynamic_SpotifyOAuth.refreshOAuth()["access_token"]
    
except:
    print(Dynamic_SpotifyOAuth.refreshOAuth())
    exit(1)

# try:
#     conn = sqlite3.connect("/Users/bryan/Desktop/Webscraper/databases/Music_Data.sqlite3")
    # cur = conn.cursor()

try:
    #  connect to the cloud database
    pool = connGCPDB.connect_with_connector("musicData")
    cur = pool.connect()
    
except:
    print("Cannot Connect to Database")
    exit(1)




epoch_time = int(time.time())
url = f"https://api.spotify.com/v1/me/player/recently-played?after={epoch_time}&limit=50"

header = {"Authorization": f"Bearer {OauthToken}"}

dataStream = requests.get(url, headers=header).text
data = json.loads(dataStream)
# print(dataStream)


if "error" in data:
    print("Error:", data["error"]["message"])
    exit(1)





#goop = dict() my goop


# # print(dataStream)
tracks = data["items"]
# aID = ((tracks[0]["track"]["artists"][0]["id"]))
fGenres = list()
# getGenre(aID)
albums = dict()
# # dtInfo = dict()
for item in tracks:
    print(item["track"]["name"])
    spotTrackID = item["track"]["id"]
    # print(item["track"]["name"], "ID:", item["track"]["id"])

    for artist in item["track"]["artists"]:
        print(artist["name"])
        aID = artist["id"]
        print("ID:", aID)
        aGenres = getGenre(aID)
        for genre in aGenres:
            cur.execute(sqlalchemy.text("INSERT IGNORE INTO Genres (name) VALUES (:name)"), parameters={"name" : genre,})
            cur.commit()
        
        fGenres = classifyGenre(aGenres)
        print(fGenres)
        loadArtistData(artist["name"], fGenres, aID)
        
        
    alName = item["track"]["album"]["name"]
    spotAlID = item["track"]["album"]["id"]
    print(alName)
    cur.execute(sqlalchemy.text("INSERT IGNORE INTO Albums (name, spotifyAlbumID) VALUES (:name , :alID)"), parameters={"name" : alName, "alID" : item["track"]["album"]["id"],})
    cur.commit()
    # print(item["track"]["album"]["name"], "ID:", item["track"]["album"]["id"])

    print("UTC",item["played_at"])
    plyAt = str(item["played_at"])
    tChart = (assignTmChrt(plyAt))
    print(tChart)

    loadTrackData(item["track"]["name"], item["track"]["album"]["id"], alName, spotTrackID, plyAt, tChart)

    #at this point all of the data from this track has been added.

    #now I add the linker table info 
    for artist in item["track"]["artists"]:  #pulls the track/album id and artist id from the database for each artist on the track
        AALinker = cur.execute(sqlalchemy.text("select Albums.id, Artists.id from Albums join Artists on Albums.spotifyAlbumID =(:alID) and Artists.name = (:artName)"), parameters={"alID" : spotAlID, "artName" : artist["name"],})
        aaTup = (AALinker.fetchall()[0])
        cur.execute(sqlalchemy.text("INSERT IGNORE INTO Artists_Albums (album_id, artist_id) values (:alID ,:artID )"), parameters={"alID" : aaTup[0], "artID" : aaTup[1],})
        cur.commit()
        

        atLinker = cur.execute(sqlalchemy.text("select Tracks.id, Artists.id from Artists join Tracks on Artists.name =(:name) and Tracks.spotifyTrackID =(:tID) order by Tracks.id DESC"), parameters= {"name" : artist["name"], "tID" :spotTrackID,})
        atTups = (atLinker.fetchall())
        
        tempList = cur.execute(sqlalchemy.text("Select * from Artists_Tracks")) #list of all track artist pairs
        tempList = tempList.fetchall()
        
      
        for atTup in atTups: 
            if atTup not in tempList: # check if this data is new
                cur.execute(sqlalchemy.text("INSERT IGNORE INTO Artists_Tracks (track_id, artist_id) values (:tID , :artID)"), parameters= {"tID" : atTup[0], "artID" : atTup[1],})
                cur.commit()
                print("adding new pair")
            else:
                # already in the db 
                continue 
            

    print()

cur.close()

print(datetime.utcnow())


