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
