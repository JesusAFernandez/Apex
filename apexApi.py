import pymongo
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


class ApexAPI(object):
    def __init__(self):
        self.uri = "mongodb+srv://apex:apexpwd@cluster0.awzetve.mongodb.net/?retryWrites=true&w=majority"
        self.conf = pyspark.SparkConf()
        self.conf.set("spark.jars.packages",
                      "org.mongodb.spark:mongo-spark-connector:10.0.3")
        self.conf.set("spark.mongodb.read.connection.uri", self.uri)
        self.conf.setAppName("apex")

        self.sc = SparkContext(conf=self.conf)
        self.sqlC = SQLContext(self.sc)

        self.routes = self.sqlC.read.format("mongodb").option("uri", self.uri).option(
            'spark.mongodb.database', 'apexDatabase').option('spark.mongodb.collection', 'routes').load()
        self.routesTable = self.routes.createOrReplaceTempView("routes")

        #self.sqlCAirport = SQLContext(self.sc)
        self.airports = self.sqlC.read.format("mongodb").option("uri", self.uri).option(
            'spark.mongodb.database', 'apexDatabase').option('spark.mongodb.collection', 'airports').load()
        self.airportsTable = self.airports.createOrReplaceTempView("airports")

        self.airlines = self.sqlC.read.format("mongodb").option("uri", self.uri).option(
            'spark.mongodb.database', 'apexDatabase').option('spark.mongodb.collection', 'airlines').load()
        self.airlinesTable = self.airlines.createOrReplaceTempView("airlines")


    def airportsInCountry(self, countryName):
        return self.sqlC.sql("SELECT Airport, Name, City, Country From airports Where Country == '" + countryName + "'")
  

    def Airlines(self, stops, codeShare=False, active=False):

        callSetup = "SELECT AirlineID From routes Where "
        count = 0

        if(stops):
            callSetup += "Stops == '" + stops + "'"
            count +=1
        if(codeShare):
            if(count):
                callSetup += "And "
            callSetup += "Codeshare == 'Y'"
            count +=1

        if(active):
            if(count == 0):
                rezB = self.sqlC.sql("SELECT Airline, Name, Country, Active from airlines Where Active == 'Y' and Country == 'United States'")
                return rezB
            else:
                return self.sqlC.sql("SELECT Airline, Name, Country, Active from airlines Where Active == 'Y' and Country == 'United States' and Airline IN("+callSetup+")")
        
            
        return self.sqlC.sql("SELECT Airline, Name, Country, Active from airlines Where Airline IN("+callSetup+")")
    
    def highestAirportCountry(self):
        return self.sqlC.sql("SELECT Country, COUNT(Country) Airport_Count FROM airports GROUP BY Country ORDER by Airport_Count DESC")

    def topKCities(self, K):
        someTable = self.sqlC.sql("SELECT SourceA as col1 from routes UNION ALL SELECT DestinationA as col1 from routes").createOrReplaceTempView("someTable")
        return self.sqlC.sql("Select City, Count(City) as Count from (select t1.col1, t2.City from airports t2 INNER JOIN someTable t1 on t1.col1 = t2.IATA) GROUP BY City ORDER BY Count DESC LIMIT "+K+"")
        

# def airLinesWithCodeShare(CSV):
#   return sqldf("SELECT Airline From CSV Where Codeshare IS 'Y'")

# def activeAirlinesInUnitedStates(CSV):
#   return sqldf("SELECT Airline From CSV Where Country IS 'United States' and Active IS 'Y'")

# def highestNumberofAirports(CSV):
#   results = sqldf("SELECT Country, COUNT(Country) mycount FROM CSV GROUP BY Country") 
#   return sqldf("SELECT Country, MAX(mycount) FROM results")

# def highestNumber(CSV, K):
#   results = sqldf("SELECT City, COUNT(City) mycount FROM CSV GROUP BY City ORDER BY mycount DESC")
#   return sqldf("SELECT * From results LIMIT '" + K + "'")

# def bestRoute(CSV, src, dest):
#   resultsA = sqldf("SELECT SourceA, DestinationA FROM CSV WHERE SourceA IS '" + src + "'")
#   resultsB = sqldf("SELECT SourceA, DestinationA FROM CSV WHERE DestinationA IS '" + dest + "'")
#   print(resultsA)

#   resultsC = sqldf("SELECT SourceA as temps, DestinationA FROM resultsB where temps IN(select DestinationA as temp from resultsA) OR temps IS '" + src + "'")
#   print(resultsC)
