from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, hour, col
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import os

spark = SparkSession.builder.appName("AnalisisBicicletas").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

ruta = "/home/janier/proyecto_bicicletas/resultados/bicicletas_limpio"
df = spark.read.option("header", True).option("inferSchema", True).csv(ruta)

print("=== DEMANDA PROMEDIO POR HORA DEL DÍA ===")
por_hora = df.groupBy("hr").agg(avg("cnt").alias("promedio")).orderBy("hr")
por_hora.show(24, truncate=False)

print("=== DEMANDA PROMEDIO POR PERIODO ===")
por_periodo = df.groupBy("periodo_dia").agg(avg("cnt").alias("promedio")).orderBy("periodo_dia")
por_periodo.show()

os.makedirs("/home/janier/proyecto_bicicletas/resultados", exist_ok=True)

datos = por_hora.collect()
horas = [int(f["hr"]) for f in datos]
promedios = [float(f["promedio"]) for f in datos]

plt.figure(figsize=(10, 5))
plt.plot(horas, promedios, marker="o")
plt.title("Demanda promedio de bicicletas por hora")
plt.xlabel("Hora del día")
plt.ylabel("Alquileres promedio")
plt.xticks(range(0, 24))
plt.grid(True)
plt.tight_layout()
plt.savefig("/home/janier/proyecto_bicicletas/resultados/demanda_por_hora.png")
plt.close()

print("Gráfica guardada: resultados/demanda_por_hora.png")
spark.stop()
