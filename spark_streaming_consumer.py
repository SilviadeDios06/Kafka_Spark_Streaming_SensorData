from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, from_unixtime
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType, TimestampType

# Crear sesión de Spark
spark = SparkSession.builder.appName("SensorDataAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Definir esquema
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", FloatType()),
    StructField("humidity", FloatType()),
    StructField("timestamp", LongType())
])

# Leer el stream desde Kafka
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sensor_data")
    .option("startingOffsets", "latest")
    .load()
)

# Parsear el JSON del mensaje
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Convertir timestamp (segundos unix) a tipo timestamp de Spark
parsed_with_ts = parsed_df.withColumn("timestamp_ts", from_unixtime(col("timestamp")).cast(TimestampType()))

# Calcular promedio por sensor cada minuto
windowed_stats = (
    parsed_with_ts
    .groupBy(window(col("timestamp_ts"), "1 minute"), "sensor_id")
    .agg({"temperature": "avg", "humidity": "avg"})
    .select(
        "window",
        "sensor_id",
        col("avg(temperature)").alias("avg_temperature"),
        col("avg(humidity)").alias("avg_humidity")
    )
)

# Mostrar resultados en consola
query = (
    windowed_stats.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
