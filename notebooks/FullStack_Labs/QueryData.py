# Databricks notebook source
spark.sql("create or replace temporary view candidates using parquet options (path \"/mnt/victormrlucasformula1dl/app/FullStackLabs-candidates.parquet\")")

# COMMAND ----------

df = spark.sql("select * from candidates")

display(df)

# COMMAND ----------

import matplotlib.pyplot as plt
labels = []
sizes = []

#Hires by technology (pie chart)
#HIRED when he has both scores greater than or equal to 7
df = spark.sql("select Technology, count(*) as Amount from candidates where CodeScore >= 7 group by Technology order by 1")
pandDF=df.select(df.Technology,df.Amount).toPandas()

labels = pandDF['Technology'].tolist()
sizes = pandDF['Amount'].tolist()

fig1, ax1 = plt.subplots(figsize=(10, 13))
ax1.pie(sizes, labels=labels, autopct='%1.1f%%',
        shadow=True, startangle=90)
ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

plt.title("Amount of Hires by technology")
plt.show()

# COMMAND ----------

import matplotlib.pyplot as plt
labels = []
sizes = []

#Hires by year (horizontal bar chart)
#HIRED when he has both scores greater than or equal to 7
df = spark.sql("select substr(Application_date,1,4) as Year, count(*) as Amount from candidates where CodeScore >= 7 group by substr(Application_date,1,4) order by 1")
pandDF=df.select(df.Year,df.Amount).toPandas()

labels = pandDF['Year'].tolist()
sizes = pandDF['Amount'].tolist()

fig1, ax1 = plt.subplots(figsize=(10, 10))
ax1.barh(labels, sizes , color='Blue')
plt.xlabel("Amount")
plt.ylabel("Years")
plt.title("Amount of Hired Candidates by Years")

plt.show()
#display(df)

# COMMAND ----------

#Hires by seniority (bar chart)
df = spark.sql("select YOE as Seniority, count(*) as Amount from candidates where CodeScore >= 7 group by YOE order by 1 desc")

display(df)