from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, isnan

spark = SparkSession.builder.appName("LimpiezaBicicletas").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

ruta = "/home/janier/proyecto_bicicletas/datos/hour.csv"
df = spark.read.option("header", True).option("inferSchema", True).csv(ruta)

print("REGISTROS ORIGINALES:", df.count())
print("REGISTROS DUPLICADOS:", df.count() - df.dropDuplicates().count())

nulos = sum(
    count(when(col(c).isNull() | isnan(col(c)), c)).alias(c)
    for c in ["temp", "atemp", "hum", "windspeed", "cnt"]
)
print("NULOS EN VARIABLES PRINCIPALES:")
df.select(nulos).show()

limpio = df.dropDuplicates().filter(
    col("cnt").isNotNull() &
    col("cnt").between(0, 10000) &
    col("temp").between(0, 1) &
    col("hum").between(0, 1)
)

limpio = limpio.withColumn(
    "periodo_dia",
    when(col("hr").between(6, 11), "Mañana")
    .when(col("hr").between(12, 17), "Tarde")
    .when(col("hr").between(18, 23), "Noche")
    .otherwise("Madrugada")
)

salida = "/home/janier/proyecto_bicicletas/resultados/bicicletas_limpio"
limpio.write.mode("overwrite").option("header", True).csv(salida)

print("REGISTROS LIMPIOS:", limpio.count())
print("ARCHIVO GUARDADO EN:", salida)
print("DEMANDA PROMEDIO:", limpio.agg({"cnt": "avg"}).first()[0])

spark.stop()
