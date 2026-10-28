from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, from_unixtime, to_timestamp,
    window, avg
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, LongType
)

spark = SparkSession.builder \
    .appName("DemandaBicicletasStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

esquema = StructType([
    StructField("sensor_id", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("rentals", IntegerType()),
    StructField("timestamp", LongType())
])

mensajes = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "latest") \
    .load()

datos = mensajes.select(
    from_json(col("value").cast("string"), esquema).alias("dato")
).select("dato.*")

datos = datos.withColumn(
    "event_time",
    to_timestamp(from_unixtime(col("timestamp")))
).filter(col("sensor_id").isNotNull())

resumen = datos.withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("sensor_id")
    ).agg(
        avg("temperature").alias("temperatura_promedio"),
        avg("humidity").alias("humedad_promedio"),
        avg("rentals").alias("demanda_promedio")
    )

consulta = resumen.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

consulta.awaitTermination()
