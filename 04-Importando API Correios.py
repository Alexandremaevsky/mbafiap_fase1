
# coding: utf-8

# 1- CArregar os dados do dataset para o datafram(ggeolocation_df)

# In[1]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, udf
from pyspark.sql.types import StructType, StringType, DoubleType
import time

spark = SparkSession.builder    .master("local")     .appName("update_geolocalization")    .enableHiveSupport()    .getOrCreate()


# In[2]:


schema = StructType()       .add("geolocation_zip_code_prefix",StringType(),True)       .add("geolocation_lat",DoubleType(),True)       .add("geolocation_lng",DoubleType(),True)       .add("geolocation_city",StringType(),True)       .add("geolocation_state",StringType(),True) 


geolocation_df = spark.read.option("header",True)                        .option("delimiter",",")                        .schema(schema)                        .csv("hdfs://namenode:8020/datalake/olist/raw/csv/olist_geolocation_dataset.csv")

print(geolocation_df.count())
print(geolocation_df.printSchema())
geolocation_df.cache()


# In[3]:


geoclocation_cep_distinct_df = geolocation_df.select("geolocation_zip_code_prefix").distinct()
geoclocation_cep_distinct_df.cache()

print(geoclocation_cep_distinct_df.count())


# In[4]:


def formatCep(cep):
    cep_formatado = cep
    
    cep_formatado.zfill(5)
    
    if(len(cep_formatado) == 5):
        cep_formatado = cep_formatado + "-000"
        
    return cep_formatado

formatCepUDF = udf(lambda z:formatCep(z),StringType())


# In[5]:


geolocation_formattedCep_distinct_df = geoclocation_cep_distinct_df.select(formatCepUDF(col("geolocation_zip_code_prefix")).alias("geolocation_zip_code_prefix"))                                                                .sort("geolocation_zip_code_prefix")

geolocation_formattedCep_distinct_df.cache()


# In[6]:


geolocation_formattedCep_distinct_df.show()
geolocation_formattedCep_distinct_df.count()


# 1- Instalando a API Correios

# In[6]:


get_ipython().system(' pip install pycep-correios')


# Consultando a API pycep_correios. Este código foi construido com base na documentação do pycep.

# In[7]:


import time
from pycep_correios import get_address_from_cep, WebService, exceptions


def addAddressByCorreios(row):    
    rowDict = row.asDict()
    cep = rowDict.get("geolocation_zip_code_prefix")
    print(cep)

    try:
        address_json = get_address_from_cep(str(cep), webservice=WebService.VIACEP)
        rowDict["geolocation_city"] = address_json["cidade"]
        rowDict["geolocation_state"] = address_json["uf"]
        rowDict["geolocation_street"] = address_json["logradouro"]
        
    except exceptions.CEPNotFound as ecnf:
        rowDict["geolocation_city"] = ''
        rowDict["geolocation_state"] = ''
        rowDict["geolocation_street"] = ''

    except exceptions.ConnectionError as errc:
        rowDict["geolocation_city"] = '-1'
        rowDict["geolocation_state"] = '-1'
        rowDict["geolocation_street"] = '-1'
        
    except exceptions.InvalidCEP as icnf:
        rowDict["geolocation_city"] = '-2'
        rowDict["geolocation_state"] = '-2'
        rowDict["geolocation_street"] = '-2'
    
    print(rowDict)
    time.sleep(4)
    return rowDict


# In[8]:


geolocation_formattedCep_rows = geolocation_formattedCep_distinct_df.collect()


# In[36]:


print(geolocation_formattedCep_rows)


# Faz a consuta do CEP quue estão no dataset 

# In[9]:


import pandas as pd

geolocation_formattedCep_address = [
    addAddressByCorreios(row) for row in geolocation_formattedCep_rows]


# Foi preciso consultar todos os ceps do dataset e salvar no csv

# In[52]:


df = spark.createDataFrame(geolocation_formattedCep_address)


# Neste passo, foi tentado executar o append na base do hive, 
# porém, por algum erro as colunas foram trocadas e não consegui resolver

# In[58]:


from pyspark.sql import HiveContext

hive_context = HiveContext(spark)
df.write.mode('append').insertInto('olist_cleansed_db.geolocation_tb')

geolocation_tb = hive_context.table("olist_cleansed_db.geolocation_tb")
geolocation_tb.show()


# Transformo dataFrame spark em um dataframe pandas para depois exportar o arquivo csv.

# In[63]:


import pandas as pd
result_df = df.select("*").toPandas()


# Salvo arquivo em csv

# In[72]:


result_df.to_csv("/mnt/notebooks/grupob/olist/geolocation_cep_correios3.csv")


# In[1]:


#CSV com o resultado do codigo acima

os.chdir(r'/mnt/notebooks/grupob/olist')

get_ipython().system(' hadoop fs -put geolocation_cep_correios3.csv /datalake/olist/raw/csv')


# In[74]:


get_ipython().system(' hadoop fs -ls /datalake/olist/raw/csv')


# In[75]:


get_ipython().system(' hdfs dfs -cat /datalake/olist/raw/csv/geolocation_cep_correios3.csv')


# Removo os arquivos testes gerado anteriormente e faço o backup do resultado da consulta do pycep

# In[11]:


get_ipython().system(' hdfs dfs -rm /datalake/olist/raw/csv/geolocation_cep_correios.csv')


# In[4]:


get_ipython().system(' hdfs dfs -mkdir /datalake/olist/raw/csv/bkp')
get_ipython().system(' hdfs dfs -cp /datalake/olist/raw/csv/geolocation_cep_correios3.csv /datalake/olist/raw/csv/bkp')
get_ipython().system(' hadoop fs -ls /datalake/olist/raw/csv/bkp')


# In[6]:


get_ipython().system(' hadoop fs -mv /datalake/olist/raw/csv/geolocation_cep_correios3.csv /datalake/olist/raw/csv/geolocation_cep_correios.csv')
get_ipython().system(' hadoop fs -ls /datalake/olist/raw/csv/')


# In[15]:


#caso der erro recupero o backup
#! hdfs dfs -cp /datalake/olist/raw/csv/bkp/geolocation_cep_correios3.csv /datalake/olist/raw/csv/
#! hadoop fs -mv /datalake/olist/raw/csv/geolocation_cep_correios3.csv /datalake/olist/raw/csv/geolocation_cep_correios.csv
#! hadoop fs -ls /datalake/olist/raw/csv/


# Precisei remover a coluna Index do CSV, por isso refiz o arquivo

# In[7]:


import pandas as pd
csv2 =pd.read_csv("/mnt/notebooks/grupob/olist/geolocation_cep_correios.csv")
csv2.to_csv("/mnt/notebooks/grupob/olist/geolocation_cep_correios.csv",index=False)


# In[8]:



os.chdir(r'/mnt/notebooks/grupob/olist')

get_ipython().system(' hadoop fs -put geolocation_cep_correios.csv /datalake/olist/raw/csv')


# In[12]:


get_ipython().system(' hadoop fs -ls /datalake/olist/raw/csv')
get_ipython().system(' hdfs dfs -cat /datalake/olist/raw/csv/geolocation_cep_correios.csv')


# In[9]:


schema = StructType()       .add("geolocation_city",StringType(),True)       .add("geolocation_state",StringType(),True)       .add("geolocation_street",StringType(),True)       .add("geo_zip_code_prefix",StringType(),True) 


geolocation_correios_df = spark.read.option("header",True)                        .option("delimiter",",")                        .schema(schema)                        .csv("hdfs://namenode:8020/datalake/olist/raw/csv/geolocation_cep_correios.csv")


# In[10]:


geolocation_correios_df.show()


# In[24]:


from pyspark.sql import HiveContext

hive_context = HiveContext(spark)
#geolocation_correios_df.write.mode('append').insertInto('olist_cleansed_db.geolocation_tb')

geolocation_tb = hive_context.table("olist_cleansed_db.geolocation_tb")
geolocation_tb.show()


# In[26]:


df=spark.sql("show databases")
df.show()


# In[27]:


df1=spark.sql("select * from olist_cleansed_db.geolocation_tb")
df1.show()


# In[14]:


geoclocation_cep_distinct_df.join(geolocation_df,geolocation_df.geolocation_zip_code_prefix == geoclocation_cep_distinct_df.geolocation_zip_code_prefix, "inner").drop(geoclocation_cep_distinct_df["geolocation_zip_code_prefix"]).count()


# In[15]:


geoclocation_cep_distinct_df.count()


# In[11]:


get_ipython().system(' pip install geopy')


# In[23]:


import time
from geopy.geocoders import Nominatim

#adding new row with address provided by geopy
def getCepByLatLon(row):    
    rowDict = row.asDict()
    
    
    city = rowDict.get("geolocation_city")
    street = rowDict.get("geolocation_state")
    print(city)
    print(street)
    
    if(street!= "" and street!="-1" and street!="-2"):
        address = "{street} {city}"
        geolocator = Nominatim(user_agent="update_geolocation")
        location = geolocator.geocode(address.format(street=street,city=city))
        print(location.latitude)
        rowDict["geolocation_lat"] = str(location.latitude)
        rowDict["geolocation_lon"] = str(location.longitude)
    time.sleep(4)
    return rowDict


# In[18]:


geolocation_valid_df = geolocation_correios_df.select("*").where(col("geo_zip_code_prefix")!="geolocation_zip_code_prefix")


# In[19]:


geolocation_valid_df.cache()


# In[20]:


geolocation_valid_df.count()


# In[21]:


geolocation_valid_df = geolocation_valid_df.select("*").where(col("geolocation_street").isNotNull())

geolocation_valid_df.show()


# In[22]:


geolocation_valid_rows = geolocation_valid_df.collect()


# In[24]:


geolocation_latlon_address = [
        getCepByLatLon(row)
        for row in geolocation_valid_rows
     ]
geolocation_latlon_address_df = spark.createDataFrame(geolocation_latlon_address)
geolocation_latlon_address_df.show()


# In[ ]:


geolocation_latlon_adress.show()

