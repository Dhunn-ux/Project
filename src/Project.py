import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

# This is spark session
spark = SparkSession.builder.appName("app_name").getOrCreate()

#  This function read the path file and seperate the coloumns
def read_csv(path: str) -> DataFrame:
    """this functon is read spark dataframe"""
    return spark.read.csv(path, header=True)


def days_of_flights(flights: DataFrame) -> DataFrame:
    flights = flights.withColumn("day", flights["day"].cast(IntegerType()))
    """this function shows the total days"""
    # total_days =  flights.groupBy("year").sum("day")
    total_days = flights.groupBy(F.col("year")).agg(F.count("day").alias("total_days"))
    return total_days


def joints_flights_planes(flights: DataFrame, planes: DataFrame) -> DataFrame:
    flights = flights.withColumn("dep_delay", flights["dep_delay"].cast(IntegerType()))
    flights = flights.withColumn("arr_delay", flights["arr_delay"].cast(IntegerType()))
    """ join flights dataframe and planes daraframe on tailnum and year coloumns"""
    flight_planes = flights.join(planes, how="inner", on=["tailnum"])

    delay = (
        flight_planes.groupBy("manufacturer")
        .sum("dep_delay", "arr_delay")
        .sort(desc("sum(arr_delay)"))
        .sort(desc("sum(dep_delay)"))
    )
    return delay

def joints_flights_airports(flights: DataFrame, airports: DataFrame) -> DataFrame:
    """this function join flights dataframe and airports daraframe on origin and IATA_CODE coloumns"""
    flight_airport = flights.join(
        airports, flights.origin == airports.IATA_CODE, "inner"
    )
    return flight_airport.select(countDistinct("CITY"))


# This is main function
def main():
    """OS.enviroment"""
    current_path = os.environ["download_data"]
    paths = os.path.join(
        os.path.abspath(os.path.dirname(__file__)),
        current_path,
    )
    flights = "flights.csv"
    planes = "planes.csv"
    airport = "airports.csv"

    flights = read_csv(f"{paths}{flights}")
    planes = read_csv(f"{paths}{planes}")
    airport = read_csv(f"{paths}{airport}")

    # - how many days does the flights table cover ?
    # --- >>>336776 days
    days = days_of_flights(flights)
    days.show()
    df_merge = joints_flights_planes(flights, planes)
    df_merge.show(1)
    #what is the relationship between flights and planes tables
    #-->>he relationship between flights and planes tables is tailnum

    # which airplane manufacturer incurred the most delays in the analysis period
    # --->>  the most delays airplane manufacturer in the analysis period is EMBRAER

    # how many departure cities the flight database covers
    #  ->>Newark and New York are the cities
    df_join = joints_flights_airports(flights, airport)
    df_join.show()


if __name__ == "__main__":
    main()
