# -*- coding: utf-8 -*-
"""
Created on Fri Jan 22 14:22:45 2021

@author: Amadou tall

projet d'analyse des données Spark Walmart
"""

#import findspark
#findspark.init("C:/spark")

# Importation
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Question1 : Start a simple Spark Session

spark = SparkSession.builder.master("local").appName("Walmart_stock").getOrCreate()

# Question2 : Load the Walmart Stock CSV File

Walmart_stock = spark.read.option("header",'true')\
                    .csv("../data/walmart_stock.csv")
Walmart_stock.show(3)

# transformation data frame to table for SQL 

Walmart_stock.createOrReplaceTempView("Walmart_stockSQL") 

# Question3 : What are the column names?
 
# DSL
Walmart_stock.columns
# SQL
spark.sql("""SHOW COLUMNS from Walmart_stockSQL""").show() 

# Question4 : What does the Schema look like?

Walmart_stock.printSchema() # nullable = true : autorise les valeurs nulles


# Question5 : Create a new dataframe with a column called HV_Ratio that is the ratio of the High Price versus volume of stock traded for a day

# DSL
Walmart_stock = Walmart_stock.withColumn("HV_Ratio", F.col("High")/F.col("Volume"))
Walmart_stock.show() 


# Question6 : What day had the Peak High in Price? 
# les deux requetes(DSL et SQL) donnent le meme résultat 

# DSL
Walmart_stock.orderBy(F.col("High").desc()).select(F.col("Date")).head() # solution 1

#Walmart_stock.select(F.col("Date"))\
        #.orderBy(F.col("High").desc())\
        #.head() # solution 2

# SQL
spark.sql("""select Date from Walmart_stockSQL order by High desc""").first() # solution 1
#spark.sql("""select Date from Walmart_stockSQL order by High desc limit 1""").show() # solution 2

# Question7 : What is the mean of the Close column?

# DSL
# Walmart_stock.select("Close") \
#        .summary("mean") \
#        .show()  # solution 1

Walmart_stock.agg(F.mean("Close").alias("Moyenne")).show() # solution 2 

# SQL
spark.sql("""select mean(Close) as Moyenne from Walmart_stockSQL""").show()

# Question8 : What is the max and min of the Volume column?

# DSL
Walmart_stock.agg(F.max("Volume"),F.min("Volume"))\
        .show()

# SQL
spark.sql("""select max(Volume), min(Volume) from Walmart_stockSQL""").show()




# Question9 : How many days was the Close lower than 60 dollars?

# DSL
#Walmart_stock.filter(F.col("Close") < '60') \
#        .agg(F.count("Date")) \ # faire l'aggregation apres le filter
#        .show()  # solution 1

Walmart_stock.filter(F.col("Close") < '60')\
        .count()   # solution 2

# SQL
spark.sql("""select count(Date) from Walmart_stockSQL where Close < '60' """).show()



# Question10 : What percentage of the time was the High greater than 80 dollars ?(In other words, (Number of Days High>80)/(Total Days in the dataset))

# DSL
Temp = Walmart_stock.filter(F.col("High")>'80')\
               .agg(F.count("*").alias("Comptage"))\
               .collect()[0][0] 
Walmart_stock.agg(F.round((Temp/F.count("*")*100),2).alias("Percentage"))\
        .show()

# SQL
spark.sql(""" select round((select count(*) from Walmart_stockSQL
                where High>='80')/(count(*)) * 100, 2 ) as Percentage  from WalmartSQL """).show()




# Question11 : What is the max High per year?

# DSL
Walmart_stock.groupBy(F.year("Date").alias("Année"))\
         .agg(F.max("High").alias("Max_High"))\
         .sort(F.year("Date"))\
         .show()

# SQL
spark.sql(""" select max(High) as max_High, substr(Date,1,4) as year
                                           from Walmart_stockSQL group by year """).show() 



# Question12 : What is the average Close for each Calendar Month? In other words, across all the years, what is the average Close price for Jan,Feb, Mar, etc... Your result will have a value for each of these months.

# DSL

Walmart_stock.select(F.col("Close"),F.month("Date").alias("Month"))\
        .groupBy(F.col("Month"))\
        .agg(F.mean(F.col("Close")).alias("Mean_Close"))\
        .orderBy("Month")\
        .show() 

# SQL
spark.sql("""select month(Date) as Month, mean(Close) as Mean_Close 
                        from Walmart_stockSQL group by Month order by Month""").show()


# Close Spark Session
spark.stop()
