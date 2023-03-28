from google.cloud.sql.connector import Connector, IPTypes
import pymysql
import sqlalchemy
import os 
from dotenv import load_dotenv

load_dotenv()

# path to the files containing the GAC
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/bryan/Desktop/Webscraper/Data Science/Sound-Footprint/silicon-monitor-373300-de2951ea01a7.json" 

pw = os.getenv("pw")

def connect_with_connector(dBase) -> sqlalchemy.engine.base.Engine: #returns a cloud sql engine

    # initialize Connector object
    
    connector = Connector(IPTypes.PUBLIC)

    # function to return the database connection
    def getconn() -> pymysql.connections.Connection:
        conn: pymysql.connections.Connection = connector.connect(
            "silicon-monitor-373300:us-central1:sound-footprint-databases",
            "pymysql",
            user="root",
            password= pw,
            db = dBase
        )
        return conn

    # create connection pool
    pool = sqlalchemy.create_engine(
        "mysql+pymysql://",
        creator= getconn
    )
    return pool


#test code

# albumIns = sqlalchemy.text("INSERT IGNORE INTO Albums (gName, spotifyAlbumID) values (:gName, :spotifyAlbumID)",)
# albumSelect = sqlalchemy.text("SELECT * FROM Albums")
# pool = connect_with_connector()

# db_conn = pool.connect()

# db_conn.execute(albumIns, parameters={"gName": "hyperchondriac", "spotifyAlbumID": "hdfbfbsbdf123"})
# db_conn.commit
# # db_conn.execute("Select * from Albums")


# result = db_conn.execute(albumSelect).fetchall()

# for item in result:
#     print(item)


# # print(db_conn)


# db_conn.close()