
vboxuser@bigdata:~$ cat kafka_producer.py
import time
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

file_path = 'household_power_consumption.txt'

print("Iniciando transmisión de datos en tiempo real...")
with open(file_path, 'r') as file:
    header = file.readline()  # Omitir cabecera
    for line in file:
        fields = line.strip().split(';')
        if len(fields) >= 9 and fields[2] != '?':
            data = {
                "sensor_id": 1,
                "temperature": float(fields[2]),  # Potencia activa global
                "humidity": float(fields[4]),     # Voltaje
                "timestamp": int(time.time())
            }
            producer.send('sensor_data', value=data)
            print(f"Enviado: {data}")
            time.sleep(1)
vboxuser@bigdata:~$
