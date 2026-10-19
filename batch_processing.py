
vboxuser@bigdata:~$ cat batch_processing.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, to_timestamp, avg, max, min, hour, month, year
from pyspark.sql.types import DoubleType
import matplotlib.pyplot as plt

# 1. Inicializar la sesión de Spark
spark = SparkSession.builder \
    .appName("UCI_Household_Power_Batch_Analysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Cargar el dataset especificando el delimitador ';' y los nulos '?'
data_path = "household_power_consumption.txt"
df_raw = spark.read.option("header", "true") \
    .option("sep", ";") \
    .option("nullValue", "?") \
    .csv(data_path)

print("--- Esquema Original ---")
df_raw.printSchema()

# 3. Limpieza y Transformación de Datos
# Combinar Date (dd/mm/yyyy) y Time (hh:mm:ss) en una columna de tipo timestamp
df_cleaned = df_raw.withColumn("timestamp", to_timestamp(concat_ws(" ", col("Date"), col("Time")), "d/M/yyyy HH:mm:ss"))

# Convertir columnas numéricas de String a Double
numeric_cols = [
    "Global_active_power", "Global_reactive_power", "Voltage",
    "Global_intensity", "Sub_metering_1", "Sub_metering_2", "Sub_metering_3"
]

for c in numeric_cols:
    df_cleaned = df_cleaned.withColumn(c, col(c).cast(DoubleType()))

# Eliminar registros con valores nulos en variables clave
df_cleaned = df_cleaned.dropna(subset=["timestamp", "Global_active_power"])

print("--- Esquema Transformado y Limpio ---")
df_cleaned.printSchema()

# 4. Análisis Exploratorio de Datos (EDA)
# Promedio y consumo máximo de potencia activa global por hora del día
hourly_analysis = df_cleaned.withColumn("hour", hour("timestamp")) \
    .groupBy("hour") \
    .agg(
        avg("Global_active_power").alias("avg_active_power"),
        max("Global_active_power").alias("max_active_power"),
        avg("Voltage").alias("avg_voltage")
    ) \
    .orderBy("hour")

print("--- Análisis Promedio de Consumo por Hora del Día ---")
hourly_analysis.show(24)

# 5. Visualización de Resultados (Generar gráfico PNG)
pandas_hourly = hourly_analysis.toPandas()

plt.figure(figsize=(10, 5))
plt.plot(pandas_hourly["hour"], pandas_hourly["avg_active_power"], marker='o', color='b', label='Potencia Activa Promedio (kW)')
plt.title("Patrón de Consumo Eléctrico Promedio por Hora del Día (UCI Dataset)")
plt.xlabel("Hora del Día (0 - 23)")
plt.ylabel("Potencia Activa Promedio (kW)")
plt.grid(True)
plt.legend()
plt.savefig("resultado_batch_consumo_hora.png")
print("Gráfico generado exitosamente como 'resultado_batch_consumo_hora.png'")

spark.stop()
