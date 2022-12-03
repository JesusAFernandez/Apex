from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
import pandas as pd

class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def basicTripRecommendations(self, cityX, cityY, stops, hops=5):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._basic_trip_recommendations, cityX, cityY, hops)
            path = ""
            pathArray = []
            for row in result:
                if row["Stops"] > int(stops):
                    continue
                for temp in row["nodesInPath"]:
                    path += temp["IATA"] + " -> "
                path = path[:-4]
                pathArray.append(path)
                path = ""
            data = {'Route Path': pathArray}
            df = pd.DataFrame(data=data)
            return df

    @staticmethod
    def _basic_trip_recommendations(tx, cityX, cityY, hops):
        query = (
           '''
           MATCH path = (start:Airport {City:$cityX})-[r:Connection*1..
           ''' +
           str(hops) +
           '''
           ]->(end:Airport {City:$cityY}) return [node in nodes(path) | properties(node)] as nodesInPath,  [r in relationships(path) | properties(r)] as relationshipsInPath, reduce(sum=0, x in relationships(path) | sum + toInteger(x.Stops)) as Stops LIMIT 25
           '''
        )
        result = tx.run(query, cityX=cityX, cityY=cityY)
        return [row.data() for row in result]