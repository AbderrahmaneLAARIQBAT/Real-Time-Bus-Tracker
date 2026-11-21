from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, countDistinct

# ⏱️ 1. إنشاء SparkSession
spark = SparkSession.builder \
    .appName("Smart Bus Real-Time Analytics") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 📂 2. قراءة بيانات الطوبيسات من HDFS
df = spark.read.parquet("hdfs://localhost:9000/user/hadoop/output/processed_data")

print(" Data loaded from HDFS")

# 🧱 3. عرض schema
df.printSchema()

# 👀 4. عرض أول 10 صفوف
df.show(10)

# 📊 5. حساب معدل السرعة لكل خط
avg_speed_per_line = df.groupBy("line_id") \
    .agg(avg("speed").alias("avg_speed"))

print("\n Average Speed per Line:")
avg_speed_per_line.show()

# 🚨 6. كشف الطوبيسات اللي السرعة ديالهم أقل من 10 كم/س
slow_buses = df.filter(col("speed") < 10)

print("\n Buses moving slower than 10 km/h:")
slow_buses.select("bus_id", "line_id", "speed", "timestamp").show()

# 🧮 7. عدد الحافلات الفعالة (اللي كيبان اسمهم فـ البيانات)
active_buses = df.select("bus_id").distinct().count()

print(f"\n Number of active buses: {active_buses}")

# 📌 8. عدد الخطوط المتوفرة
lines = df.select("line_id").distinct().count()
print(f" Number of bus lines: {lines}")

# ❌ إيقاف Spark
spark.stop()
