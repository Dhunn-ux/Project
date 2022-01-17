import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import IntegerType




#1- create new branch and merge it with master---done
#2-#define every parameter and function-----done
#3-converting_in_int and maxvalue in a one func---done
#4-#max is function name...dont use functions name in variable and parameter---done
#5- correct spelling---done
#6- use hard coded path example - OS--
#7-make the write(save) function--done
#8- Use underscore betwen large words-  --done
spark = SparkSession.builder.appName('app_name').getOrCreate()



def read_csv(path: str)-> DataFrame:
    return spark.read.csv(path, sep='\t',header=True)


def joins(basic:DataFrame,rating:DataFrame)-> DataFrame:
    return basic.join(rating,basic.tconst ==  rating.tconst,"inner")



def movie(more_than_hundred:DataFrame)->DataFrame:
    return more_than_hundred.filter(more_than_hundred.titleType == 'movie')

# converting_in_int and maxvalue in a one func
def converting_in_int(movie:DataFrame)->DataFrame:

    floating = movie.withColumn("numVotes", movie["numVotes"].cast(IntegerType()))
    ordering = floating.orderBy(['numVotes'], ascending = [False])
    more_than_hundred = ordering.filter(ordering.numVotes > 100)
    return more_than_hundred


def movies(more_than_hundred: DataFrame) -> DataFrame:
    return more_than_hundred.filter(more_than_hundred.titleType == 'movie')

#max is function name...Ch

def columns(part: DataFrame) -> DataFrame:
    return  part.select("primaryTitle")


def main():
    current = os.path.abspath(os.path.dirname(__file__))
    file_path  = "C:\\Users\\Rev07\\Downloads\\"
    paths = (os.path.join(current, file_path,))

    basic = read_csv(f"{paths}ratings\\data.tsv")
    rating =read_csv(f"{paths}basics\\data.tsv")
    inner_joins = joins(basic,rating)
    moviecoloum = movie(inner_joins)
    converting_into_int = converting_in_int(moviecoloum)
    coloum_select = columns(converting_into_int)
    coloum_select.show(15)
    # coloum_select.write.csv('rating_file.csv')

    # "C:\\Users\\Rev07\\Downloads\\"))


if __name__ == '__main__':
    main()





