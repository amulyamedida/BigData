from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[2]").getOrCreate()

lines = spark.readStream.format("socket") \
    .option("host", "ec2-13-204-43-4.ap-south-1.compute.amazonaws.com") \
    .option("port", 1234).load()
res=(lines.withColumn("name", split(col("value"),",")[0])
     .withColumn("age", split(col("value"),",")[1])
     .withColumn("city", split(col("value"),",")[2])
     .drop("value")
     )

host="jdbc:mysql://mysqldb.c1sms2y0qice.ap-south-1.rds.amazonaws.com:3306/ammudb"
#res.writeStream.outputMode("append").format("console").start().awaitTermination()
def foreach_batch_function(df, epoch_id):
    df=df.withColumn("ts",current_timestamp())
    df.write.mode("append").format("jdbc").option("url",host).option("user","admin").option("password","Amulya.9391").option("dbtable","structtab").save()
    pass

res.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()
