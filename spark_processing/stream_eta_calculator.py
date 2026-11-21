from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, expr
from pyspark.sql.types import DoubleType
import math

# ✅ إنشاء SparkSession
spark = SparkSession.builder \
    .appName("ETA Calculator") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 📥 قراءة بيانات GPS من HDFS
df_gps = spark.read.parquet("hdfs://localhost:9000/user/hadoop/output/processed_data")

# 📥 قراءة محطات الطوبيس من CSV
df_stations = spark.read.option("header", True).csv("data/stations.csv") \
    .withColumn("latitude", col("latitude").cast("double")) \
    .withColumn("longitude", col("longitude").cast("double"))

# 📌 دالة لحساب المسافة بين نقطتين (Haversine Formula)
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return R * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))

udf_haversine = udf(haversine, DoubleType())

# 🔗 Join GPS مع المحطات حسب نفس الخط
df_joined = df_gps.alias("gps").join(
    df_stations.alias("st"),
    col("gps.line_id") == col("st.line_id")
)

# ➕ حساب المسافة بين كل حافلة وكل محطة فالخط ديالها
df_with_distance = df_joined.withColumn(
    "distance_km",
    udf_haversine(
        col("gps.latitude"), col("gps.longitude"),
        col("st.latitude"), col("st.longitude")
    )
)

# 🟢 اختيار أقرب محطة لكل حافلة
df_min_distance = df_with_distance.groupBy("bus_id") \
    .agg(expr("min(distance_km)").alias("min_distance"))

# 🔁 إعادة join باش نرجعو تفاصيل المحطة الأقرب
df_eta = df_with_distance.join(df_min_distance, on="bus_id") \
    .filter(col("distance_km") == col("min_distance")) \
    .withColumn("eta_minutes", col("distance_km") / (col("speed") / 60)) \
    .select(col("gps.bus_id").alias("bus_id"), col("gps.line_id").alias("line_id"), col("st.station_name").alias("station_name"), col("distance_km"), col("gps.speed").alias("speed"), col("eta_minutes"))

    

# 💾 تخزين النتائج فـ HDFS
df_eta.write.mode("overwrite").parquet("hdfs://localhost:9000/smartbus/eta_results")

print("[OK] ETA results saved to HDFS")
spark.stop()
