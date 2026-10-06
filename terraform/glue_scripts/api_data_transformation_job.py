import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "source_path", "target_path", "zone_lookup_path"],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
# Reruns only overwrite the partitions they touch (safe for Airflow retries)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = args["source_path"]
TARGET_PATH = args["target_path"].rstrip("/")
ZONE_LOOKUP_PATH = args["zone_lookup_path"]  # TLC taxi_zone_lookup.csv on S3

MIN_TRIPS_PER_GROUP = 20  # ignore tiny groups so the gold tables aren't noise
MAX_SPEED_MPH = 80
MAX_TRIP_HOURS = 6

INT_COLS = ["vendorid", "passenger_count", "ratecodeid", "pulocationid", "dolocationid", "payment_type"]
DOUBLE_COLS = [
    "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee",
]
STRING_COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "store_and_fwd_flag"]


def write_parquet(df, name, partition_by=None):
    writer = df.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(f"{TARGET_PATH}/{name}/")


# ---------------------------------------------------------------------------
# 1. Read raw JSON and enforce types (Socrata returns everything as strings
#    and omits null fields, so add any missing columns first)
# ---------------------------------------------------------------------------
raw = spark.read.json(SOURCE_PATH)
for c in INT_COLS + DOUBLE_COLS + STRING_COLS:
    if c not in raw.columns:
        raw = raw.withColumn(c, F.lit(None).cast("string"))

typed = raw.select(
    *[F.col(c).cast("double").cast("int").alias(c) for c in INT_COLS],
    *[F.col(c).cast("double").alias(c) for c in DOUBLE_COLS],
    F.to_timestamp("tpep_pickup_datetime").alias("pickup_ts"),
    F.to_timestamp("tpep_dropoff_datetime").alias("dropoff_ts"),
    "store_and_fwd_flag",
)

deduped = typed.dropDuplicates(
    ["vendorid", "pickup_ts", "dropoff_ts", "pulocationid", "dolocationid", "trip_distance", "total_amount"]
)

# ---------------------------------------------------------------------------
# 2. Derived fields
# ---------------------------------------------------------------------------
duration_min = (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60

enriched = (
    deduped
    .withColumn("duration_min", F.round(duration_min, 2))
    .withColumn(
        "avg_mph",
        F.when(F.col("duration_min") > 0, F.round(F.col("trip_distance") / (F.col("duration_min") / 60), 1)),
    )
    .withColumn("pickup_date", F.to_date("pickup_ts"))
    .withColumn("pickup_hour", F.hour("pickup_ts"))
    .withColumn("day_of_week", F.date_format("pickup_ts", "E"))
    .withColumn(
        "distance_band",
        F.when(F.col("trip_distance").isNull(), F.lit("unknown"))
        .when(F.col("trip_distance") < 1, "0-1 mi")
        .when(F.col("trip_distance") < 3, "1-3 mi")
        .when(F.col("trip_distance") < 10, "3-10 mi")
        .otherwise("10+ mi"),
    )
    .withColumn("tip_pct", F.when(F.col("fare_amount") > 0, F.col("tip_amount") / F.col("fare_amount")))
    .withColumn(
        "is_airport_trip",
        F.coalesce(F.col("ratecodeid").isin(2, 3) | (F.col("airport_fee") > 0), F.lit(False)),
    )
)

# ---------------------------------------------------------------------------
# 3. Data quality rules -> quarantine with reason codes
#    (a row can break several rules; reasons are comma-separated)
# ---------------------------------------------------------------------------
rules = {
    "null_timestamp": F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull(),
    "dropoff_before_pickup": F.col("dropoff_ts") < F.col("pickup_ts"),
    "trip_too_long": F.col("duration_min") > MAX_TRIP_HOURS * 60,
    "negative_amount": (F.col("fare_amount") < 0) | (F.col("total_amount") < 0),
    "zero_distance_with_fare": (F.col("trip_distance") == 0) & (F.col("fare_amount") > 0),
    "impossible_speed": F.col("avg_mph") > MAX_SPEED_MPH,
    "bad_passenger_count": F.col("passenger_count").isNull()
    | (F.col("passenger_count") < 1) | (F.col("passenger_count") > 6),
    "missing_pickup_zone": F.col("pulocationid").isNull(),
}

flagged = enriched.withColumn(
    "reject_reason",
    F.concat_ws(",", *[F.when(cond, F.lit(name)) for name, cond in rules.items()]),
).cache()

clean = flagged.filter(F.col("reject_reason") == "")
quarantine = flagged.filter(F.col("reject_reason") != "")

# ---------------------------------------------------------------------------
# 4. Enrich clean trips with zone names (small dimension -> broadcast join)
# ---------------------------------------------------------------------------
zones = spark.read.option("header", True).csv(ZONE_LOOKUP_PATH).select(
    F.col("LocationID").cast("int").alias("zone_id"),
    F.col("Borough").alias("borough"),
    F.col("Zone").alias("zone"),
)
pu_zones = F.broadcast(zones.select(
    F.col("zone_id").alias("pulocationid"),
    F.col("borough").alias("pu_borough"),
    F.col("zone").alias("pu_zone"),
))
do_zones = F.broadcast(zones.select(
    F.col("zone_id").alias("dolocationid"),
    F.col("borough").alias("do_borough"),
    F.col("zone").alias("do_zone"),
))
clean = clean.join(pu_zones, "pulocationid", "left").join(do_zones, "dolocationid", "left")

# ---------------------------------------------------------------------------
# 5. Gold tables
# ---------------------------------------------------------------------------
# 5a. Where and when do trips earn the most per minute?
zone_hour = (
    clean
    .filter(F.col("pu_borough").isNotNull() & (F.col("pu_borough") != "Unknown"))
    .groupBy("pu_borough", "pu_zone", "day_of_week", "pickup_hour")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("total_amount"), 2).alias("avg_fare"),
        F.round(F.sum("total_amount") / F.sum("duration_min"), 2).alias("revenue_per_min"),
        F.round(F.avg("avg_mph"), 1).alias("avg_mph"),
    )
    .filter(F.col("trips") >= MIN_TRIPS_PER_GROUP)
)
slot_window = Window.partitionBy("day_of_week", "pickup_hour").orderBy(F.desc("revenue_per_min"))
zone_hour = zone_hour.withColumn("rank_in_slot", F.dense_rank().over(slot_window))

# 5b. Tipping behaviour (credit card only: cash tips aren't recorded)
tipping = (
    clean
    .filter((F.col("payment_type") == 1) & (F.col("fare_amount") > 0))
    .groupBy("pu_borough", "pickup_hour", "distance_band", "is_airport_trip")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.avg("tip_pct") * 100, 2).alias("avg_tip_pct"),
        F.round(F.percentile_approx("tip_pct", 0.5) * 100, 2).alias("median_tip_pct"),
        F.round(F.avg(F.when(F.col("tip_amount") == 0, 1).otherwise(0)) * 100, 1).alias("pct_no_tip"),
    )
    .filter(F.col("trips") >= MIN_TRIPS_PER_GROUP)
)

# 5c. Data quality scorecard (rows can fail several rules, so pcts won't sum to 100)
total_rows = flagged.count()
dq_scorecard = (
    quarantine
    .withColumn("rule", F.explode(F.split("reject_reason", ",")))
    .groupBy("rule")
    .agg(F.count("*").alias("rows_failed"))
    .withColumn("total_rows", F.lit(total_rows))
    .withColumn("pct_of_total", F.round(F.col("rows_failed") / F.col("total_rows") * 100, 3))
    .withColumn("run_date", F.current_date())
)

# ---------------------------------------------------------------------------
# 6. Write everything as Parquet
# ---------------------------------------------------------------------------
write_parquet(clean, "silver/clean_trips", partition_by=["pickup_date"])
write_parquet(quarantine, "silver/quarantine_trips", partition_by=["pickup_date"])
write_parquet(zone_hour.coalesce(1), "gold/zone_hour_earnings")
write_parquet(tipping.coalesce(1), "gold/tipping_behavior")
write_parquet(dq_scorecard.coalesce(1), "gold/dq_scorecard")

print(f"Total rows read (after dedupe): {total_rows}")
print(f"Rejected rows: {quarantine.count()}")

job.commit()