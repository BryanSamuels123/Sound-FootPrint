import requests
import urllib.parse
import base64
import time
import connGCPDB
import os
import sqlalchemy


try:
    pool = connGCPDB.connect_with_connector("tokenData")
    db_conn = pool.connect()

except:
    print("CANNOT CONNECT TO MYSQL DATABASE")
    exit(1)

# conn = sqlite3.connect("/Users/bryan/Desktop/Webscraper/databases/Spotify_Tokens.sqlite3")
# cur = conn.cursor()

clientID = os.getenv("clientID")

clientSecret = os.getenv("clientSecret")

uri = "http://127.0.0.1:5500/Data%20Science/index.html"

encodedURI = urllib.parse.quote(uri,safe="")


url = f"https://accounts.spotify.com/authorize?client_id={clientID}&response_type=code&redirect_uri={encodedURI}&show_dialogue=false&scope=user-read-recently-played"

# print(url, "\n")


myID_mySecret = clientID + ":" + clientSecret
tempBytes = myID_mySecret.encode("ascii")

b64Bytes = base64.b64encode(tempBytes)
encodedIdSEC = b64Bytes.decode("ascii")


def getAccess():
    code = "AQADPrCZkcT88SSN1Z0B9C38VMqrDPioYC6iepFnspZCMcjhDA2S3KWpgeP86Z8Gn74a9CCFs-iZzFiJaMxlLodyGjFtgkNRqQ8JtpEdrTZUt9K00mYcYTAT2VLs5GP4q7U2aBbwj1dlQY_JqmXGAThi4iLMdC1cflfb6vk9tCc-pd_WOQ7rPE2M9SFV7VjgRQ86a8fqpQec1uyoc-fSXHCtPRCMz7WkpNLObAP3"

    postURL = "https://accounts.spotify.com/api/token"

    # use regular un-encoded URI
    postData = {"grant_type": "authorization_code", "code": code, "redirect_uri": uri, "client_id": clientID, "client_secret": clientSecret}
    postHeaders = {"Content-Type": "application/x-www-form-urlencoded", "Authorization": f"Basic {encodedIdSEC}"}

    resp = requests.post(postURL, data=postData, headers=postHeaders)
    try: 
        db_conn.execute(sqlalchemy.text("INSERT INTO Tokens (access_token, refresh_token, Date_Created) values (:access_token, :refresh_token, current_timestamp())"), parameters={ "acces_token" : resp.json()["access_token"], "refresh_token" : resp.json()["refresh_token"],})
        db_conn.commit()
    except:
        print(resp.json())

    db_conn.close()
    return(resp.json())
    
# optomized to work with cloud sql
def refreshOAuth():
    result = db_conn.execute(sqlalchemy.text("SELECT (refresh_token) FROM Tokens ORDER BY (Date_Created) DESC"))
    refTok = (result.fetchone()[0])

    

    postURL = "https://accounts.spotify.com/api/token"


    myID_mySecret = clientID + ":" + clientSecret
    tempBytes = myID_mySecret.encode("ascii")
    b64Bytes = base64.b64encode(tempBytes)
    encodedIdSEC = b64Bytes.decode("ascii")


    # use regular un-encoded URI
    postData = {"grant_type": "refresh_token", "refresh_token": refTok}
    postHeaders = {"Content-Type": "application/x-www-form-urlencoded", "Authorization": f"Basic {encodedIdSEC}"}

    resp = requests.post(postURL, data=postData, headers=postHeaders)

    try: 
        db_conn.execute(sqlalchemy.text("INSERT INTO Tokens (access_token, refresh_token, Date_Created) values (:access_token, :refresh_token, current_timestamp())"), parameters={"access_token" : resp.json()["access_token"], "refresh_token" : refTok,})
        db_conn.commit()
    except:
        print(resp.json())

    db_conn.close()
    return(resp.json())




# tokens = refreshOAuth()

# for (k,v) in tokens.items():
#     print(k,v)
# print()

