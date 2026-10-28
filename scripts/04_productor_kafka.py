import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Enviando datos de bicicletas. Ctrl+C para detener.")

try:
    while True:
        dato = {
            "sensor_id": random.choice(
                ["estacion_01", "estacion_02", "estacion_03"]
            ),
            "temperature": round(random.uniform(18, 32), 1),
            "humidity": round(random.uniform(35, 85), 1),
            "rentals": random.randint(0, 20),
            "timestamp": int(datetime.now(timezone.utc).timestamp())
        }
        producer.send("sensor_data", dato)
        producer.flush()
        print("Enviado:", dato)
        time.sleep(2)
except KeyboardInterrupt:
    print("Productor detenido.")
finally:
    producer.close()
