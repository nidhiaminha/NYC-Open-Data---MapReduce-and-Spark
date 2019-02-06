from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from operator import add
import sys
APP_NAME = "NYPD"

def main(spark,filename):
    #Cleaning the data by selecting required columns
    spark_dataf=spark.read.csv(filename,header=False);
    cleaned_df=spark_dataf.select("_c0","_c2","_c3","_c10","_c11","_c12","_c13","_c14","_c15","_c16","_c17","_c24")
    #Selecting the rows, that are not null from the cleaned data
    cleaned_data=cleaned_df.where("_c0 is not null and _c2 is not null and _c3 is not null and _c10 is not null and _c11 is not null and _c12 is not null and _c14 is  not null and _c15 is not null and _c16 is not null and _c17 is not null and _c24 is not null and  _c13 is not null")
    #register as a table to perform data anaylis using spark sql, Storing the notnull required columns in a Dataframe
    cleaned_data.registerTempTable("Cleaned_table")
    #load and write a file, Load the data from the Dataframe and write it into a file
    temp_csv=spark.sql("select _c0,_c2,_c3,_c10,_c11,_c12,_c13,_c14,_c15,_c16,_c17,_c24 from Cleaned_table")
    temp_csv.write.format("csv").save("/user/aminhani/result_spark/OutputSpark")
    
    #Date on which maximum number of accidents took place, Computing date on which max_num accidents took place
    Sparktask2a=spark.sql("select _c0,count(*) as total from Cleaned_table group by _c0 order by total desc  limit 1")
    Sparktask2a.write.format("csv").save("/user/aminhani/result_spark/Sparktask2.1")
    #Borough with maximum count of accident fatality
    Sparktask2b=spark.sql("select _c2,sum(int(_c11+_c13+_c15+_c17)) as total from Cleaned_table group by _c2 order by total desc  limit 1")
    Sparktask2b.write.format("csv").save("/user/aminhani/result_spark/Sparktask2.2")
    #Zip with maximum count of accident fatality
    Sparktask2c=spark.sql("select _c3,sum(int(_c11+_c13+_c15+_c17)) as total from Cleaned_table group by _c3 order by total desc  limit 1")
    Sparktask2c.write.format("csv").save("/user/aminhani/result_spark/Sparktask2.3")
    #Which vehicle type is involved in maximum accidents
    Sparktask2d=spark.sql("select _c24,count(*) as total from Cleaned_table group by _c24 order by total desc  limit 1")
    Sparktask2d.write.format("csv").save("/user/aminhani/result_spark/Sparktask2.4")
    #Year in which maximum Number Of Persons and Pedestrians Injured
    Sparktask2e=spark.sql("select year(cast(unix_timestamp(_c0, 'MM/dd/yyyy') as timestamp)) as Year,sum(int(_c12+_c10)) as total from Cleaned_table group by `Year` order by total desc limit 1")
    Sparktask2e.write.format("csv").save("/user/aminhani/result_spark/Sparktask2.5")
    #Year in which maximum Number Of Persons and Pedestrians Killed
    Sparktask2f=spark.sql("select year(cast(unix_timestamp(_c0, 'MM/dd/yyyy') as timestamp)) as Year,sum(int(_c11+_c13)) as total from Cleaned_table group by `Year` order by total desc limit 1")
    Sparktask2f.write.format("csv").save("/user/aminhani/result_spark/Sparktask2.6")
    #Year in which maximum Number Of Cyclist Injured and Killed (combined)
    Sparktask2g=spark.sql("select year(cast(unix_timestamp(_c0, 'MM/dd/yyyy') as timestamp)) as Year,sum(int(_c14+_c15)) as total from Cleaned_table group by `Year` order by total desc limit 1")
    Sparktask2g.write.format("csv").save("/user/aminhani/result_spark/Sparktask2.7")
    #Year in which maximum Number Of Motorist Injured and Killed (combined)
    Sparktask2h=spark.sql("select year(cast(unix_timestamp(_c0, 'MM/dd/yyyy') as timestamp)) as Year,sum(int(_c16+_c17)) as total from Cleaned_table group by `Year` order by total desc limit 1")
    Sparktask2h.write.format("csv").save("/user/aminhani/result_spark/Sparktask2.8")


if __name__ == "__main__":
   # Configure Spark
   spark=SparkSession.builder.master("local").appName(APP_NAME).getOrCreate()
   filename=sys.argv[1]
   # Execute Main functionality
   main(spark,filename)