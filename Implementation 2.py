# Databricks notebook source
#Importing essential functions

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import explode
from pyspark.sql import functions as func

# COMMAND ----------

#Loading the dataset

csv_skill_50k = spark.read.format("csv") \
        .option("header", "false") \
        .load("dbfs:/FileStore/tables/skill2vec_50K.csv.gz")

# COMMAND ----------

#Loading necessary functions

from pyspark.sql.functions import array, col, explode, lit, array_except, expr, countDistinct, desc, lower 

# COMMAND ----------

#Pre-processing

#Assigning column names

csv_skill_50k_columns = csv_skill_50k.columns

#Converting to array

list_csv_skill_50k = csv_skill_50k.select(array(csv_skill_50k_columns).alias("csv_skill_50k_list"))

#Removing null values

non_null_list_csv_skill_50k = list_csv_skill_50k.withColumn("not_null",array_except("csv_skill_50k_list",array(lit(None))))

#Removing csv_skill_50k_list column

non_null_list_csv_skill_50k = non_null_list_csv_skill_50k.drop("csv_skill_50k_list")

#Adding JD column to the dataframe

non_nullDF = non_null_list_csv_skill_50k.withColumn('JD',non_null_list_csv_skill_50k['not_null'][0])

#Adding Skills column to the dataframe

non_nullDF=non_nullDF.withColumn("Skills",expr("slice(not_null,2,size(not_null))"))

#Removing not_null column 

non_nullDF= non_nullDF.drop("not_null")

#Creating final dataframe

Final_Job_Data = non_nullDF.select("JD", explode("Skills").alias("Skills"))

# COMMAND ----------

#Q1

#Mapping every row to a value 1 and then reducing it to get the final count

dual_Implementation_Q1=non_null_list_csv_skill_50k.rdd.map(lambda row: 1).reduce(lambda x, y: x + y)
print(dual_Implementation_Q1)

# COMMAND ----------

#Q2
#mapping each row to a tuple of (Skills, 1)
#summing up the counts for each unique skill
#sorting the count in descending order
#converting rdd to dataframe

dual_ImpQ2 = Final_Job_Data.rdd.map(lambda row: (row["Skills"], 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1], ascending=False) \
    .toDF(["Skills", "count"])
dual_ImpQ2.show(10)

# COMMAND ----------

#Q3

#every job description is mapped to a count of skills
#then reduced to frequency of each count

dual_ImpQ3 = Final_Job_Data.rdd.map(lambda row: (row["JD"], 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .map(lambda a: a[1]) \
    .map(lambda count: (count, 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1], ascending=False) \
    .toDF(["Num_Skills", "count"])
dual_ImpQ3.show(5)

# COMMAND ----------

#Q4

#RDD is mapped to a tuple with the value "Skills" in lowercase and a count of 1 and counting each distinct lowercased skills 

dual_ImpQ4 = Final_Job_Data.rdd.map(lambda row: (row["Skills"].lower(), 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1], ascending=False) \
    .toDF(["Skills", "count"])
dual_ImpQ4.show(10)

# COMMAND ----------

#loading O*NET file

Tech_Skills_Onet = spark.read \
    .option("sep", "\t") \
    .option("header", "true") \
    .csv("dbfs:/FileStore/tables/Technology_Skills.txt")

# COMMAND ----------

#loading before_join df from the Final_Job_Data dataframe and converting skills column to lowercase

before_join_df = Final_Job_Data.withColumn("Skills", lower(Final_Job_Data["Skills"]))

# COMMAND ----------

#Q5

#creating teamp view

Tech_Skills_Onet.createOrReplaceTempView("Tech_Skills")
before_join_df.createOrReplaceTempView("Before_join")
Final_Job_Data.createOrReplaceTempView("Final_Job_Data")

# creating new temp view with lowercase skills 

spark.sql("SELECT lower(Example) AS Skills FROM Tech_Skills").createOrReplaceTempView("Tech_Skills_lower")

# joining the views

final_df = spark.sql("""
    SELECT jd.Skills, ds.Skills AS Example
    FROM Before_join jd
    JOIN Tech_Skills_lower ds ON lower(jd.Skills) = ds.Skills
""")

# counting the number of instances

dual_Q5_1 = spark.sql("SELECT COUNT(*) FROM Final_Job_Data").first()[0]
dual_Q5_2 = final_df.count()

# 
print("Before join:", dual_Q5_1)
print("After join:", dual_Q5_2)


# COMMAND ----------

#convertint the original dataset to lowercase

df_skills_jd = before_join_df.select(lower(col("Skills")).alias("Skills"))

# COMMAND ----------

# creating temporary views

Tech_Skills_Onet.createOrReplaceTempView("Tech_Skills_Onet")
df_skills_jd.createOrReplaceTempView("df_skills_jd")

# joining the views

joined_6_df = spark.sql("""
    SELECT df.Skills, sd.`Commodity Title`
    FROM df_skills_jd df
    JOIN Tech_Skills_Onet sd ON lower(df.Skills) = lower(sd.Example)
""")

# Counting the Commodity Titles

commodity_title = joined_6_df.groupBy("`Commodity Title`").count()

# Sorting in descending order

final_count = commodity_title.orderBy(desc("count"))

#selecting top 10 results

top_10 = final_count.limit(10)

# Displaying the top 10 titles

top_10.show(truncate=False)


# COMMAND ----------


