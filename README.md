# Análisis de la demanda de bicicletas con Apache Spark y Kafka

## 1. Descripción
Proyecto académico de procesamiento de datos por lotes y en tiempo real.
Se utiliza el conjunto de datos Bike Sharing de UCI para analizar la demanda
de alquiler de bicicletas. Kafka transmite datos simulados de estaciones y
Spark Structured Streaming calcula promedios por estación en ventanas de tiempo.

## 2. Tecnologías
- Python
- Apache Spark 3.5.8 y PySpark
- Apache Kafka 3.9.2
- kafka-python
- Pandas y Matplotlib

## 3. Dataset
Bike Sharing Dataset, repositorio UCI Machine Learning:
https://archive.ics.uci.edu/dataset/275/bike+sharing+dataset

El archivo hour.csv contiene registros horarios de alquileres de bicicletas.
La columna cnt representa el total de alquileres registrados por hora.

## 4. Estructura del proyecto
- datos/: archivos originales del dataset
- scripts/01_explorar_datos.py: exploración inicial con Spark
- scripts/02_limpiar_datos.py: limpieza de datos
- scripts/03_analisis.py: análisis y gráfica de demanda
- scripts/04_productor_kafka.py: productor de datos simulados
- scripts/05_spark_streaming.py: procesamiento en tiempo real
- resultados/: datos procesados y gráfica
- evidencias/: capturas de las pruebas

## 5. Requisitos
- Java 11
- Apache Spark 3.5.8
- Apache Kafka 3.9.2
- Python del entorno virtual spark311-env
- Paquetes kafka-python, pandas y matplotlib

## 6. Ejecución del procesamiento por lotes
Desde la carpeta del proyecto y con el entorno virtual activado:

```bash
spark-submit scripts/01_explorar_datos.py
spark-submit scripts/02_limpiar_datos.py
spark-submit scripts/03_analisis.py
```

La limpieza y el análisis utilizan los datos guardados en resultados/.

## 7. Ejecución del procesamiento en tiempo real
1. Iniciar el servidor Kafka configurado en la máquina.
2. Crear el tema si todavía no existe:

```bash
/opt/kafka/bin/kafka-topics.sh --create --topic sensor_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

3. En una terminal, activar el entorno e iniciar el productor:

```bash
source ~/spark311-env/bin/activate
python scripts/04_productor_kafka.py
```

4. En otra terminal, ejecutar Spark Streaming:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 scripts/05_spark_streaming.py
```

El conector de Kafka se descarga si no está disponible localmente.
El productor genera datos simulados de temperatura, humedad y alquileres.
Spark calcula sus promedios por estación en ventanas de un minuto.

## 8. Resultados
El procesamiento por lotes explora y limpia los datos y genera una gráfica
de demanda por hora. El procesamiento en tiempo real recibe mensajes desde
Kafka y presenta promedios por estación.

Los datos transmitidos en tiempo real son simulados para fines académicos;
no corresponden a mediciones reales de estaciones.

## 9. Evidencias
Las capturas de ejecución deben incorporarse a la carpeta evidencias/.

## 10. Referencias
- UCI Machine Learning Repository. Bike Sharing Dataset.
  https://archive.ics.uci.edu/dataset/275/bike+sharing+dataset
- Apache Spark Documentation. https://spark.apache.org/docs/3.5.8/
- Apache Kafka Documentation. https://kafka.apache.org/documentation/
