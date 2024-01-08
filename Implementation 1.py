# Databricks notebook source
#Importing essential functions

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import explode
from pyspark.sql import functions as func

# COMMAND ----------

#loading the dataset

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

#Q1 counting number of rows
non_null_list_csv_skill_50k.count()

# COMMAND ----------

Final_Job_Data.show()

# COMMAND ----------

#Q2

#grouping by skills and counting the nunmber of individual skills

Answer_Q2 = Final_Job_Data.groupby("Skills").count().sort(desc("count"))

# COMMAND ----------

Answer_Q2.show(10)

# COMMAND ----------

#Q3

#Grouping by JD column and counting the number of instances

Question3 = Final_Job_Data.groupBy("JD").count()

#Removing JD column from dataset

Question3 = Question3.drop("JD")

#Renaming count column as Num_skills

Question3 = Question3.selectExpr("count as Num_skills")

#Grouping by Num_skills, counting the number of skills, sorting in decreasing order

Answer3= Question3.groupBy("Num_skills").count().sort(desc("count")).show(5)

# COMMAND ----------

#Q4

#converting skills column to lowercase

question_4 = Final_Job_Data.withColumn("Skills", lower(Final_Job_Data["Skills"]))

#grouping by skills column, counting the number of instance and sorting in decreasing order

Answer_4 = question_4.groupby("Skills").count().sort(desc("count"))

#printing top 10 skills

Answer_4.show(10)

# COMMAND ----------

#loading O*NET file

Tech_Skills_Onet = spark.read \
    .option("sep", "\t") \
    .option("header", "true") \
    .csv("dbfs:/FileStore/tables/Technology_Skills.txt")

# COMMAND ----------

#Q5

#converting Example column to lowercase

question_5 = Tech_Skills_Onet.withColumn("Example", lower(Tech_Skills_Onet["Example"]))

#Changing name of example column to Skills

question_5 = question_5.select("Example")
question_5 = question_5.selectExpr("Example as Skills")


# COMMAND ----------

#taking the dataframe from question 4 and converting the skills column to lowercase

df_skills_jd = question_4.select(lower(col("Skills")).alias("Skills"))
df_skills_jd.show()

# COMMAND ----------

#joining the two dataframes 

joined_final_df = df_skills_jd.join(question_5, "Skills")

# COMMAND ----------

#counting the result 

before_join = Final_Job_Data.count()
after_join = joined_final_df.count()
print("Before join:", before_join)
print("After join:", after_join)

# COMMAND ----------

#Q6

#converting example column to lowercase

question_6 = Tech_Skills_Onet.withColumn("Example", lower(Tech_Skills_Onet["Example"]))
question_6 = question_6.withColumnRenamed("Example","Skills")
question_6.show()

# COMMAND ----------

#joining the two dataframes

joined_6_df = df_skills_jd.join(question_6, "Skills")

# COMMAND ----------

#grouping and counting the commodity title

commodity_title = joined_6_df.groupBy("Commodity Title").count()

# COMMAND ----------

#sorting in descending order

final_count = commodity_title.orderBy(desc("count"))

# COMMAND ----------

#extracting top 10 results

top_10 = final_count.limit(10)

# COMMAND ----------

top_10.show(truncate=False)

# COMMAND ----------


