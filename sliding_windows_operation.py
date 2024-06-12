import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import window,col, sum, min,max, avg,current_timestamp,expr,row_number,count
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,TimestampType

spark = SparkSession.builder.appName("streaming").getOrCreate()

from pyspark.sql.types import DateType, DoubleType
schema = StructType([
    StructField("Invoice", IntegerType(), True),
    StructField("StockCode", Integer Type(), True),
    StructField("Name", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", DateType(), nullable=True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerId", IntegerType(), True),
    StructField("Country", StringType(), True),
    StructField("InvoiceTimestamp", TimestampType(), nullable=True),
])

streamingDF = spark.readStream.option("header","true").schema(schema).csv("/FileStore/tables")

dbutils.fs.rm("/FileStore/tables", True)
windowed_sum = streamingDF.groupBy(window("InvoiceTimestamp", "15 minute", "5 minute"), col("Name")).agg(sum("UnitPrice"))
windowed_sum.display()

windowed_sum = streamingDF.groupBy(window("InvoiceTimestamp", "15 minute", "5 minute"), col("Country")).agg(sum("UnitPrice"))
windowed_sum.display()

windowed_sum = streamingDF.groupBy(window("InvoiceTimestamp", "15 minute", "5 minute"), col("Country")).agg(count("UnitPrice"))
windowed_sum.display()

windowed_sum = streamingDF.groupBy(window("InvoiceTimestamp", "15 minute", "5 minute")).agg(max("UnitPrice"))
windowed_sum.display()

windowed_sum = streamingDF.groupBy(window("InvoiceTimestamp", "15 minute", "5 minute")).agg(min("UnitPrice"))
windowed_sum.display()

windowed_df = streamingDF.groupBy(window("InvoiceTimestamp", "15 minute", "5 minute")).agg(avg("UnitPrice"))
query = windowed_df.writeStream.outputMode("update").format("console").trigger(processingTime="30 seconds").start()
windowed_df.display()

query.awaitTermination (600)

spark.stop()



