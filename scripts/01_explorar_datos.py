from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("NPK-Demanda-Bicicletas-Exploracion")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

ruta = "/home/janier/proyecto_bicicletas/datos/hour.csv"

# Cargar el archivo CSV y reconocer automáticamente los encabezados.
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(ruta)
)

print("\n========== 1. ESQUEMA DEL DATASET ==========")
df.printSchema()

print("\n========== 2. TOTAL DE REGISTROS ==========")
print(f"Total de registros: {df.count()}")

print("\n========== 3. NOMBRES DE LAS COLUMNAS ==========")
print(df.columns)

print("\n========== 4. PRIMERAS 10 FILAS ==========")
df.show(10, truncate=False)

print("\n========== 5. ESTADÍSTICAS DE LA DEMANDA ==========")
df.select(
    col("cnt"),
    col("casual"),
    col("registered")
).describe().show()

spark.stop()
