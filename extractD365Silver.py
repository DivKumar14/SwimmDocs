# Databricks notebook source
# DBTITLE 1,Get params from Workflow Widgets
import os

container = dbutils.secrets.get(scope="dataplatform-secrets",key="databricks-storage-container")
env = dbutils.secrets.get(scope="dataplatform-secrets",key="databricks-environment")
storageAccount = dbutils.secrets.get(scope="dataplatform-secrets",key="databricks-storage-account")
tableNameSilver = dbutils.widgets.get("tableNameSilver")
tableNameBronze = dbutils.widgets.get("tableNameBronze")
blobPathCDC = os.path.join(f"/mnt/{storageAccount}",dbutils.widgets.get("blobPathCDC"))
blobPathDBO = os.path.join(f"/mnt/{storageAccount}",dbutils.widgets.get("blobPathDBO"))
unitySchema = dbutils.widgets.get("schema")
fullLoad = dbutils.widgets.get("fullLoad")
catalogName = dbutils.widgets.get("catalogName")
mergeId = dbutils.widgets.get("mergeId")
mergeDate = dbutils.widgets.get("mergeDate")
useUnityCatalog = dbutils.widgets.get("useUnityCatalog")
unityCatalogContainer = f"abfss://{container}@{storageAccount}.dfs.core.windows.net"
dbfsContainer = f"dbfs:/mnt/{storageAccount}/{container}"
partitionedColumn = dbutils.widgets.get("partitionedColumn")
partionedIfGenerated = dbutils.widgets.get("partionedIfGenerated")
print(f"{tableNameSilver} job : Extracting data from bronze delta to silver")

# COMMAND ----------

# DBTITLE 1,Function to extract schema and map data types
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, LongType, DoubleType, DecimalType, TimestampType, BooleanType, BinaryType
from delta.tables import *
def extractSchema(table_name,table_type) :
    if table_type == 'cdc':
        if env == 'prod':
            schemaFile = f"{blobPathCDC}/{table_name.upper()}.cdm.json"
        else:    
            schemaFile = f"{blobPathCDC}/{table_name}.cdm.json"
    if table_type == 'dbo':
        schemaFile = f"{blobPathDBO}/{table_name}.cdm.json"
    df1 = spark.read.option("multiline","true").json(schemaFile)
    df2=df1.select(explode(df1.definitions))
    df3=df2.select(col("col.*"))
    df4=df3.select(explode(col("hasAttributes")))
    df_Schema= df4.select(col("col.*"))
    column_names=df_Schema.select(df_Schema.name,df_Schema.dataFormat).toPandas()
    column_names_dict= column_names.to_dict('records')
    column_names_dict = [d for d in column_names_dict if d['name'] not in ('_SysRowId', 'LSN')]
    type_mapping = {
    'Int32': 'INT',
    'Int64': 'BIGINT',
    'Double': 'DOUBLE',
    'Decimal': 'DECIMAL',
    'DateTime': 'TIMESTAMP',
    'Boolean': 'BOOLEAN',
    'Binary': 'BINARY',
}
    
    struct_type_mapping = {
    'Int32': IntegerType(),
    'Int64': LongType(),
    'Double': DoubleType(),
    'Decimal': DecimalType(),
    'DateTime': TimestampType(),
    'Boolean': BooleanType(),
    'Binary': BinaryType(),
}
    schema = StructType([StructField(col['name'], struct_type_mapping.get(col['dataFormat'], StringType()), True) for col in column_names_dict])
    column_defs  = ', '.join([f"{col['name']} {type_mapping.get(col['dataFormat'], 'STRING')}" for col in column_names_dict])
    
    if(partitionedColumn):
        if(partionedIfGenerated):
            create_table_statement = f"CREATE TABLE IF NOT EXISTS {catalogName}.{unitySchema}.{tableNameSilver} ({column_defs},{partitionedColumn} STRING ) PARTITIONED BY ({partitionedColumn})"
        else:
            create_table_statement = f"CREATE TABLE IF NOT EXISTS {catalogName}.{unitySchema}.{tableNameSilver} ({column_defs}) PARTITIONED BY ({partitionedColumn})"
    else:
        create_table_statement = f"CREATE TABLE IF NOT EXISTS {catalogName}.{unitySchema}.{tableNameSilver} ({column_defs})"
    if useUnityCatalog == 'No':
        folderName = tableNameSilver.replace('_','-')
        create_table_statement = create_table_statement.replace(f'PARTITIONED BY ({partitionedColumn})', '')
        create_table_statement += f""" USING delta
            OPTIONS (
            'path' = '/mnt/{storageAccount}/{container}/{env}-silver/{folderName}'
            )
            """
        if(partitionedColumn):
            create_table_statement += f" PARTITIONED BY ({partitionedColumn})" 
    return {'schemaList' : schema , 'createTableStatement' : create_table_statement}

# COMMAND ----------

# DBTITLE 1,Create Silver layer table if not exist
spark.sql(extractSchema(tableNameBronze,'dbo')['createTableStatement'])

# COMMAND ----------

# DBTITLE 1,Helper Functions
def makeBlobs(path):
    # make blobs in the mounted path
    try:
      dbutils.fs.ls(path)
      print(f"{path} already exist")
    except:
      dbutils.fs.mkdirs(path)
      print(f"{path} created")
        
def resetCheckpoint(checkpoint_dir):
    # Stop any existing stream that may be using the checkpoint directory
    for stream in spark.streams.active:
        if stream.options.get("checkpointLocation") == checkpoint_dir:
            stream.stop()

    # Clear the checkpoint directory
    dbutils.fs.rm(checkpoint_dir, True)
    makeBlobs(checkpoint_dir)
    
def getDeltaTablePath(catalog_name,unity_schema,table_name):
  #Get Delta Table path for tables in unity catalog
    table_location = spark.sql(f"DESCRIBE DETAIL {catalog_name}.{unity_schema}.{table_name}") \
        .select("location") \
        .collect()[0][0]
    return table_location.replace(unityCatalogContainer, f"/mnt/{storageAccount}/{container}")  if useUnityCatalog == 'Yes' else table_location.replace('dbfs:','')      

# COMMAND ----------

# DBTITLE 1,Create Silver table checkpoint

silverCheckpoint = f"{getDeltaTablePath(catalogName,'silver',tableNameSilver)}/_checkpoint/"
makeBlobs(silverCheckpoint)

# COMMAND ----------

# DBTITLE 1,Function to merge Bronze CDC data to silver
from pyspark.sql.functions import col
from pyspark.sql.window import Window

def mergeBronzeToSilver(df, epoch_id):
    column_mapping = {column.name: column.dataType for column in extractSchema(tableNameBronze,'cdc')['schemaList']}
    source_df_casted = df.select([col(c).cast(column_mapping[c]).alias(c) for c in df.columns])
    w = Window.partitionBy(mergeId).orderBy(col(mergeDate).desc())
    source_df = source_df_casted.withColumn("row_num", row_number().over(w))
    latest_source_df = source_df.filter((col("row_num") == 1) & col(mergeId).rlike("^[0-9]*$"))
    delta_table_destination = DeltaTable.forPath(spark, getDeltaTablePath(catalogName,'silver',tableNameSilver))
    # max_merge_date = delta_table_destination.toDF().agg({mergeDate: 'max'}).collect()[0][0]
    # latest_source_df = latest_source_df.filter(col(mergeDate) > max_merge_date)
    latest_source_df = latest_source_df.drop("row_num")
    if(partionedIfGenerated):
        latest_source_df  = latest_source_df.withColumn(partitionedColumn, eval(partionedIfGenerated))
    target_columns = [column.name for column in delta_table_destination.toDF().schema]    
    insert_values = {col_name: col(f"source.{col_name}") for col_name in latest_source_df.columns if col_name in target_columns}
    merge_condition = f"target.{mergeId} = source.{mergeId}"
    delta_table_destination.alias("target") \
        .merge(latest_source_df.alias("source"), merge_condition) \
        .whenMatchedUpdateAll(condition=col("source.dml_action") == "AFTER_UPDATE") \
        .whenMatchedDelete(condition=col("source.dml_action") == "DELETE") \
        .whenNotMatchedInsert(values=insert_values,condition=col("source.dml_action") != "DELETE") \
        .execute()

# COMMAND ----------

# DBTITLE 1,Streaming Logic
if fullLoad == 'True':
    #Merge Bronze table after nightly full load to silver
#     spark.sql(f"TRUNCATE TABLE {catalogName}.{unitySchema}.{tableName}_{tableType}")
    delta_table_source = DeltaTable.forPath(spark, getDeltaTablePath(catalogName,'bronze',f"d365_{tableNameBronze}_dbo"))
    delta_table_data = delta_table_source.toDF()
    data_filtered=delta_table_data.filter(col(f"{mergeId}").rlike("^[0-9]*$"))
    columns_list = [column.name for column in extractSchema(tableNameBronze,'dbo')['schemaList']]
    column_mapping = {column.name: column.dataType for column in extractSchema(tableNameBronze,'dbo')['schemaList']}
    data_selected_source = data_filtered.select(*columns_list)
    data_casted_source = data_selected_source.select([col(c).cast(column_mapping[c]).alias(c) for c in data_selected_source.columns])
    if(partionedIfGenerated):
        data_casted_source = data_casted_source.withColumn(partitionedColumn,eval(partionedIfGenerated))
    data_casted_source.write \
    .format("delta") \
    .mode("overwrite") \
    .save(getDeltaTablePath(catalogName,'silver',tableNameSilver))
if fullLoad == 'False':
    schemaCDC = StructType([StructField(field.name, StringType(), True) for field in (extractSchema(tableNameBronze,'cdc')['schemaList']).fields])
    #Continue to load and merge from cdc bronze by streaming from a delta table as source
    readStreamDf = (spark.readStream \
                    .format("delta") \
                    .load(getDeltaTablePath(catalogName,'bronze',f"d365_{tableNameBronze}_cdc")))
    writeStreaming = (readStreamDf.writeStream \
                      .format("delta") \
                      .foreachBatch(mergeBronzeToSilver)  \
                      .option("checkpointLocation", silverCheckpoint) \
                      .trigger(once=True) \
                      .start())
