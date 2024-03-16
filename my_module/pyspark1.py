from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType



spark = SparkSession.builder.appName("sample10").getOrCreate()


athletes_data=spark.read.format("csv").option("header","true").load("file:///D:/MLA/athletes.csv")
athletes_data.show()
athletes_data.printSchema()
coaches_data=spark.read.format("csv").option("header","true").load("file:///D:/MLA/coaches.csv")
coaches_data.show()
#coaches_data.printSchema()
medals_data=spark.read.format("csv").option("header","true").load("file:///D:/MLA/Medals.csv")
medals_data.show()
#medals_data.printSchema()
EntriesGender_data=spark.read.format("csv").option("header","true").load("file:///D:/MLA/EntriesGender.csv")
EntriesGender_data.show()
#EntriesGender_data.printSchema()
Teams_data=spark.read.format("csv").option("header","true").load("file:///D:/MLA/Teams.csv")
Teams_data.show()
#Teams_data.printSchema()

medals_data1=medals_data \
    .withColumn("Gold", medals_data.Gold.cast('integer')) \
    .withColumn("Silver",medals_data.Silver.cast('integer')) \
    .withColumn("Bronze",medals_data.Bronze.cast('integer')) \
    .withColumn("Total",medals_data.Total.cast('integer'))

#medals_data1.printSchema()

from pyspark.sql.functions import col


#medals_data1 = medals_data.withColumn("Gold", medals_data.Gold.cast('integer'))
#medals_data1.printSchema()
athletes_coaches=athletes_data.join(coaches_data, athletes_data["Discipline"] == coaches_data["Discipline"],"inner")
athletes_coaches.show()

medals_teams=medals_data1.join(Teams_data, medals_data1["Team_country"] == Teams_data["Country"],"inner")
medals_teams.show()

medals_teams1 = medals_teams.orderBy(desc("Rank by Total"))
medals_teams1.show()


medals_teams1.write.format("parquet").mode("overwrite").save("D:///output/transformed")