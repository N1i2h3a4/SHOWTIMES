# Reading a JSON file and displaying the contents of the DataFrame
df=spark.read.option("multiline", "true").json("/FileStore/tables/showtimes_everyday_in_th-3.json")
display(df)
--------------------------------------------------------------------------------------------------------------------
# Flattening the JSON file

from pyspark.sql.types import *
from pyspark.sql.functions import *

def flatten(df):  
   complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   while len(complex_fields)!=0:
      col_name=list(complex_fields.keys())[0]
      print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))
    
      if (type(complex_fields[col_name]) == StructType):
         expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
         df=df.select("*", *expanded).drop(col_name)
    
      elif (type(complex_fields[col_name]) == ArrayType):    
         df=df.withColumn(col_name,explode_outer(col_name))
      
      complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   return df
---------------------------------------------------------------------------------------------------------------------------
# Display Flattened file

df_f = flatten(df)
display(df_f)
---------------------------------------------------------------------------------------------------------------------------
 # To display the contents of a DataFrame 
df.show()
---------------------------------------------------------------------------------------------------------------------------
# To see the columns present in the dataframe
df_f.printSchema()

# Display the column names
columns = df_flatten.columns
print(columns)
-------------------------------------------------------------------------------------------------------------------------------
# Created a veiw
%sql
Create temporary view showt
using json
options(path="/FileStore/tables/showtimes_everyday_in_th-3.json", multiline=true)
------------------------------------------------------------------------------------------------------------------------------------
# Seeing the veiw
%sql
select * from showt
-------------------------------------------------------------------------------------------------------------------------------------
# Trend of the movie count released in multiple language per month/year.
from pyspark.sql.functions import year, month, countDistinct

df_f_r = df_f.groupBy(year("results_release_date").alias("year"), month("results_release_date").alias("month"), "results_language") \
              .agg(countDistinct("results_title").alias("movie_count")) \
              .orderBy("year", "month")

df_f_r.show()
---------------------------------------------------------------------------------------------------------------------------------------
# Trend of the movie count release in english only per month/year 

from pyspark.sql.functions import year, month, countDistinct

df_result = df_f.filter(df_f.results_language == "English") \
              .groupBy(year("results_release_date").alias("year"), month("results_release_date").alias("month")) \
              .agg(countDistinct("results_title").alias("movie_count")) \
              .orderBy("year", "month")

df_result.show()

-----------------------------------------------------------------------------------------------------------------------------------------------

# Create a report with top 4 artists whith number of movie release per each year?(also consider unique movie names only)

from pyspark.sql.functions import year, countDistinct
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Filter for unique movie names per artist
df_unique_movies = df_f.dropDuplicates(["results_title", "results_details_cast"])

# Calculate the count of unique movies per artist and year
df_artist_count = df_unique_movies.groupBy("results_details_cast", year("results_release_date").alias("year")) \
                                 .agg(countDistinct("results_title").alias("movie_count"))

# Rank the artists based on the movie count per year
window_spec = Window.partitionBy("year").orderBy(F.desc("movie_count"))
df_ranked_artists = df_artist_count.withColumn("rank", F.row_number().over(window_spec))

# Filter for top 4 artists per year
df_top_artists = df_ranked_artists.filter(F.col("rank") <= 4).orderBy("year", "rank")

df_top_artists.show()

------------------------------------------------------------------------------------------------------------------------------------------------
# Give me the top 4 month with most number of releases?

from pyspark.sql.functions import year, month, count

df_top_months = df_f.groupBy(year("results_release_date").alias("year"), month("results_release_date").alias("month")) \
                  .agg(count("results_title").alias("movie_count")) \
                  .orderBy("movie_count", ascending=False) \
                  .limit(4)

df_top_months.show()

