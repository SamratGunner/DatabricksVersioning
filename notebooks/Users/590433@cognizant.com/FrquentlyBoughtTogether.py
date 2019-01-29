# Databricks notebook source
jdbcHostname = "azurepaassqlserver.database.windows.net"
jdbcDatabase = "OnlineShopping"
jdbcPort = 1433
jdbcUsername = dbutils.secrets.get(scope = "secrets01", key = "jdbcusername")
jdbcPassword = dbutils.secrets.get(scope = "secrets01", key = "jdbcpassword")

# COMMAND ----------

jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

product_query = "(select * from product) prod_alias"
dfProduct = spark.read.jdbc(url=jdbcUrl, table=product_query, properties=connectionProperties)
display(dfProduct)

# COMMAND ----------

orderDatabase = "ECommerceOrders"
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, orderDatabase)

# COMMAND ----------

order_query = "(select * from order_details) order_alias"
dfOrder = spark.read.jdbc(url=jdbcUrl, table=order_query, properties=connectionProperties)
display(dfOrder)

# COMMAND ----------

dfJoin = dfProduct.join(dfOrder, dfProduct.product_id == dfOrder.product_id)
dfSelect = dfJoin.select("order_id","sku")
display(dfSelect)

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.fs.mount(
# MAGIC   source = dbutils.secrets.get(scope = "secrets01", key = "blobstorageaccname"),
# MAGIC   mountPoint = dbutils.secrets.get(scope = "secrets01", key = "mountpoint"),
# MAGIC   extraConfigs = Map("fs.azure.account.key.ecommercestore.blob.core.windows.net" -> dbutils.secrets.get(scope = "secrets01", key = "storagekey")))

# COMMAND ----------

dbutils.fs.mv("dbfs:/FileStore/data1.csv/part-00000-tid-6096585814884017302-4cfd9de9-ffff-4307-b601-dbb88582c915-455-c000.csv", "dbfs:/FileStore/odr.csv")


# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/data1.csv/",True)

# COMMAND ----------

dbutils.fs.mv("dbfs:/FileStore/odr.csv","/mnt/frequent/odr.csv")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/"))

# COMMAND ----------

display(dbutils.fs.help())

# COMMAND ----------

dfSelect.coalesce(1).write.option("header", "true").option("delimiter",",").csv("dbfs:/FileStore/order1.csv")
fileName = dbutils.fs.ls("/FileStore/order1.csv").map(_.name).filter(r => r.startsWith("part"))(0)
fileLoc = "dbfs:/FileStore/" + fileName
dbutils.fs.mv(fileLoc, "dbfs:/FileStore/order1.csv/"+fileName)
dbutils.fs.rm("/mnt/frequent/order1.csv", true)
display(dbutils.fs.ls("dbfs:/FileStore"))

# COMMAND ----------

sparkDF = spark.read.format('csv').options(header='true', inferSchema='true').load('dbfs:/FileStore/data2.csv/part-00000-tid-5686502976947796506-b996684f-7cf2-492d-b6cd-5796f260b686-472-c000.csv')
display(sparkDF)

# COMMAND ----------

dbutils.fs.unmount("/mnt/frequent")