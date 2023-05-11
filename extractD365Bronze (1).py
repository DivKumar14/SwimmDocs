# Databricks notebook source
import os

container = dbutils.secrets.get(scope="dataplatform-secrets",key="databricks-storage-container")
env = dbutils.secrets.get(scope="dataplatform-secrets",key="databricks-environment")
storageAccount = dbutils.secrets.get(scope="dataplatform-secrets",key="databricks-storage-account")
tableName = dbutils.widgets.get("tableName")
bronzeTableName = f"d365_{tableName}"
blobPathCDC = os.path.join(f"/mnt/{storageAccount}",dbutils.widgets.get("blobPathCDC"))
blobPathDBO = os.path.join(f"/mnt/{storageAccount}",dbutils.widgets.get("blobPathDBO"))
unitySchema = dbutils.widgets.get("schema")
fullLoad = dbutils.widgets.get("fullLoad")
tableType= dbutils.widgets.get("tableType")
catalogName = dbutils.widgets.get("catalogName")
useUnityCatalog = dbutils.widgets.get("useUnityCatalog")
unityCatalogContainer = f"abfss://{container}@{storageAccount}.dfs.core.windows.net"
dbfsContainer = f"dbfs:/mnt/{storageAccount}/{container}"
blobPath = blobPathDBO if tableType == 'dbo' else blobPathCDC
if env == 'prod' and tableType == 'cdc':
    tableName = tableName.upper()
print(f"{tableName} job : Extracting data from path : {blobPath}")


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, StringType
from delta.tables import *
def extractSchema(table_name) :
    schemaFile = f"{blobPath}/{table_name}.cdm.json"
    df1 = spark.read.option("multiline","true").json(schemaFile)
    df2=df1.select(explode(df1.definitions))
    df3=df2.select(col("col.*"))
    df4=df3.select(explode(col("hasAttributes")))
    df_Schema= df4.select(col("col.*"))
    column_names=df_Schema.select(df_Schema.name).toPandas()['name']
    column_names_list=list(column_names)
    schema = StructType([StructField(col, StringType(), True) for col in column_names_list])
    if useUnityCatalog == 'Yes':
        create_table_statement = f"CREATE TABLE IF NOT EXISTS {catalogName}.{unitySchema}.{bronzeTableName}_{tableType} ({', '.join([f'{col} STRING' for col in column_names_list])})"
    else:
        folderName = f"{bronzeTableName}_{tableType}".replace('_','-')
        create_table_statement = f"""CREATE TABLE IF NOT EXISTS {catalogName}.{unitySchema}.{bronzeTableName}_{tableType} ({', '.join([f'{col} STRING' for col in column_names_list])}) 
        USING delta
        OPTIONS (
        'path' = '/mnt/{storageAccount}/{container}/{env}-bronze/{folderName}'
        )
        """

    return {'schemaList' : schema , 'createTableStatement' : create_table_statement}

# COMMAND ----------

spark.sql(extractSchema(tableName)['createTableStatement'])

# COMMAND ----------

def makeBlobs(path):
    try:
      dbutils.fs.ls(path)
      print(f"{path} already exist")
    except:
      dbutils.fs.mkdirs(path)
      print(f"{path} created")
def getDeltaTablePath(catalog_name,unity_schema,table_name):
    table_location = spark.sql(f"DESCRIBE DETAIL {catalog_name}.{unity_schema}.{table_name}") \
        .select("location") \
        .collect()[0][0]
    return table_location.replace(unityCatalogContainer, f"/mnt/{storageAccount}/{container}")  if useUnityCatalog == 'Yes' else table_location.replace('dbfs:','')      

# COMMAND ----------

delta_table_path = getDeltaTablePath(catalogName,unitySchema,f"{bronzeTableName}_{tableType}")
bronzeCheckpoint = f"{delta_table_path}/_checkpoint/"
makeBlobs(bronzeCheckpoint)
landingZoneLocation = f"{blobPath}/{tableName}/"

# COMMAND ----------

def resetCheckpoint(checkpoint_dir):
    # Stop any existing stream that may be using the checkpoint directory
    for stream in spark.streams.active:
        if stream.options.get("checkpointLocation") == checkpoint_dir:
            stream.stop()

    # Clear the checkpoint directory
    dbutils.fs.rm(checkpoint_dir, True)
    makeBlobs(bronzeCheckpoint)

# COMMAND ----------

dfBronze = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .schema(extractSchema(tableName)['schemaList']) \
  .load(landingZoneLocation)

# COMMAND ----------

if fullLoad == 'True':
    print(f"Reseting the checkpoint for {bronzeCheckpoint}")
    resetCheckpoint(bronzeCheckpoint)
else:
    print(f"Continue with previous checkpoint")

# COMMAND ----------


def mergeToDelta(df, epoch_id):
  delta_table = DeltaTable.forPath(spark, getDeltaTablePath(catalogName,unitySchema,f"{bronzeTableName}_{tableType}"))
  delta_table.alias("target") \
    .merge(df.alias("source"), "source.recid = target.recid") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

if (tableType == 'dbo' and fullLoad == 'True'):
    spark.sql(f"TRUNCATE table {catalogName}.{unitySchema}.{bronzeTableName}_{tableType}")
    writeStreaming = (dfBronze.writeStream \
                      .format("delta") \
                      .trigger(once=True) \
                      .option("checkpointLocation", bronzeCheckpoint) \
                      .outputMode("append") \
                      .toTable(f"{catalogName}.{unitySchema}.{bronzeTableName}_{tableType}"))
elif (tableType == 'cdc') :
     writeStreaming = (dfBronze.writeStream \
                      .format("delta") \
                      .trigger(once=True) \
                      .option("checkpointLocation", bronzeCheckpoint) \
                      .toTable(f"{catalogName}.{unitySchema}.{bronzeTableName}_{tableType}"))
elif tableType == 'dbo' and fullLoad == 'False':
    writeStreaming = (dfBronze.writeStream \
                      .format("delta") \
                      .foreachBatch(mergeToDelta) \
                      .trigger(once=True) \
                      .option("checkpointLocation", bronzeCheckpoint) \
                      .start())
# writeStreaming.awaitTermination()
# print(f'No of rows processed : {writeStreaming.status["numInputRows"]}')
