import MySQLdb

db = MySQLdb.connect(host="0.0.0.0", user="root", passwd="root")

# Create a Cursor object to execute queries.
cursor = db.cursor()

cursor.execute("""CREATE DATABASE IF NOT EXISTS Registry""")

cursor.execute("""USE Registry""")


cursor.execute("""CREATE TABLE IF NOT EXISTS Platform(
    PlatformId VARCHAR(50) PRIMARY KEY,
    PlatformName VARCHAR(30) ,
    PlatformType VARCHAR(30) ,
    PlatformHost VARCHAR(30) ,
    PlatformPort INT,
    PlatformStatus VARCHAR(20),
    LastResponse DOUBLE)""")

cursor.execute("""CREATE TABLE IF NOT EXISTS IoTSource(
    SourceId VARCHAR(50) PRIMARY KEY,
    EndPoint VARCHAR(150),
    SourceStatus VARCHAR(20),
    Description VARCHAR(200),
    SourceType VARCHAR(20),
    Label VARCHAR(100),
    PlatformId VARCHAR(50),
    LocalId VARCHAR (50),
    FOREIGN KEY(PlatformId) REFERENCES Platform(PlatformId))""")


cursor.execute("""CREATE TABLE IF NOT EXISTS Thing(
    ThingGlobalId VARCHAR(50) PRIMARY KEY,
    ThingName VARCHAR(50),
    FOREIGN KEY(ThingGlobalId) REFERENCES IoTSource(SourceId))""")


cursor.execute("""CREATE TABLE IF NOT EXISTS Metric(
    MetricId VARCHAR(150) PRIMARY KEY,
    SourceId VARCHAR(100),
    MetricName VARCHAR(50),
    MetricType VARCHAR(20),
    Unit VARCHAR(20),
    MetricDomain VARCHAR(20),
    MetricStatus VARCHAR(20),
    MetricLocalId VARCHAR (50),
    FOREIGN KEY(SourceId) REFERENCES IoTSource(SourceId))""")


db.commit()
cursor.close()