import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
import pandas as pd

import pyspark.sql.functions as F
import pyspark.sql.types as T

import pyspark.sql.functions as F
from pyspark.sql.types import *

import sys
if sys.version > '3':
    basestring = str


class ApexAPI(object):
    def __init__(self):
        self.uri = "mongodb+srv://apex:apexpwd@cluster0.awzetve.mongodb.net/?retryWrites=true&w=majority"
        self.conf = pyspark.SparkConf()
        self.conf.set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.3,graphframes:graphframes:0.8.2-spark3.2-s_2.12")
        self.conf.set("spark.mongodb.read.connection.uri", self.uri)
        self.conf.setAppName("apex")

        self.sc = SparkContext(conf=self.conf)
        self.sqlC = SQLContext(self.sc)

        self.routes = self.sqlC.read.format("mongodb").option("uri", self.uri).option(
            'spark.mongodb.database', 'apexDatabase').option('spark.mongodb.collection', 'routes').load()
        self.routesTable = self.routes.createOrReplaceTempView("routes")

        # self.sqlCAirport = SQLContext(self.sc)
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

        if (stops):
            callSetup += "Stops == '" + stops + "'"
            count += 1
        if (codeShare):
            if (count):
                callSetup += "And "
            callSetup += "Codeshare == 'Y'"
            count += 1

        if (active):
            if (count == 0):
                rezB = self.sqlC.sql(
                    "SELECT Airline, Name, Country, Active from airlines Where Active == 'Y' and Country == 'United States'")
                return rezB
            else:
                return self.sqlC.sql("SELECT Airline, Name, Country, Active from airlines Where Active == 'Y' and Country == 'United States' and Airline IN("+callSetup+")")

        return self.sqlC.sql("SELECT Airline, Name, Country, Active from airlines Where Airline IN("+callSetup+")")

    def highestAirportCountry(self):
        return self.sqlC.sql("SELECT Country, COUNT(Country) Airport_Count FROM airports GROUP BY Country ORDER by Airport_Count DESC")

    def topKCities(self, K):
        someTable = self.sqlC.sql(
            "SELECT SourceA as col1 from routes UNION ALL SELECT DestinationA as col1 from routes").createOrReplaceTempView("someTable")
        return self.sqlC.sql("Select City, Count(City) as Count from (select t1.col1, t2.City from airports t2 INNER JOIN someTable t1 on t1.col1 = t2.IATA) GROUP BY City ORDER BY Count DESC LIMIT "+K+"")