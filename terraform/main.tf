locals {
  # If you already have a variable or resource for this bucket, reference that instead.
  taxi_data_bucket = "bright-first-s3-bucket-demo-563649768086-us-east-1-an"

  # Matches the DAG's transform --target_path
  transform_root = "s3://${local.taxi_data_bucket}/landing_zone/transformation_results/transformed_city_taxi_data"

  # Columns written by the transform job into silver/clean_trips.
  # pickup_date is NOT here: it is the partition column (it lives in the folder names).
  clean_trips_columns = [
    { name = "dolocationid", type = "int" },
    { name = "pulocationid", type = "int" },
    { name = "vendorid", type = "int" },
    { name = "passenger_count", type = "int" },
    { name = "ratecodeid", type = "int" },
    { name = "payment_type", type = "int" },
    { name = "trip_distance", type = "double" },
    { name = "fare_amount", type = "double" },
    { name = "extra", type = "double" },
    { name = "mta_tax", type = "double" },
    { name = "tip_amount", type = "double" },
    { name = "tolls_amount", type = "double" },
    { name = "improvement_surcharge", type = "double" },
    { name = "total_amount", type = "double" },
    { name = "congestion_surcharge", type = "double" },
    { name = "airport_fee", type = "double" },
    { name = "pickup_ts", type = "timestamp" },
    { name = "dropoff_ts", type = "timestamp" },
    { name = "store_and_fwd_flag", type = "string" },
    { name = "duration_min", type = "double" },
    { name = "avg_mph", type = "double" },
    { name = "pickup_hour", type = "int" },
    { name = "day_of_week", type = "string" },
    { name = "distance_band", type = "string" },
    { name = "tip_pct", type = "double" },
    { name = "is_airport_trip", type = "boolean" },
    { name = "reject_reason", type = "string" },
    { name = "pu_borough", type = "string" },
    { name = "pu_zone", type = "string" },
    { name = "do_borough", type = "string" },
    { name = "do_zone", type = "string" },
  ]
}

resource "aws_glue_catalog_database" "ny_taxi" {
  name        = "ny_taxi_db"
  description = "NYC yellow taxi pipeline: silver and gold tables"
}

resource "aws_glue_catalog_table" "clean_trips" {
  name          = "clean_trips"
  database_name = aws_glue_catalog_database.ny_taxi.name
  description   = "Cleaned and enriched yellow taxi trips (silver layer), partitioned by pickup_date"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  partition_keys {
    name = "pickup_date"
    type = "date"
  }

  storage_descriptor {
    location      = "${local.transform_root}/silver/clean_trips/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    dynamic "columns" {
      for_each = local.clean_trips_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}



locals {
  zone_hour_earnings_columns = [
    { name = "pu_borough", type = "string" },
    { name = "pu_zone", type = "string" },
    { name = "day_of_week", type = "string" },
    { name = "pickup_hour", type = "int" },
    { name = "trips", type = "bigint" },
    { name = "total_revenue", type = "double" },
    { name = "avg_fare", type = "double" },
    { name = "revenue_per_min", type = "double" },
    { name = "avg_mph", type = "double" },
    { name = "rank_in_slot", type = "int" },
  ]

  tipping_behavior_columns = [
    { name = "pu_borough", type = "string" },
    { name = "pickup_hour", type = "int" },
    { name = "distance_band", type = "string" },
    { name = "is_airport_trip", type = "boolean" },
    { name = "trips", type = "bigint" },
    { name = "avg_tip_pct", type = "double" },
    { name = "median_tip_pct", type = "double" },
    { name = "pct_no_tip", type = "double" },
  ]

  dq_scorecard_columns = [
    { name = "rule", type = "string" },
    { name = "rows_failed", type = "bigint" },
    { name = "total_rows", type = "bigint" },
    { name = "pct_of_total", type = "double" },
    { name = "run_date", type = "date" },
  ]
}

resource "aws_glue_catalog_table" "zone_hour_earnings" {
  name          = "zone_hour_earnings"
  database_name = aws_glue_catalog_database.ny_taxi.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  storage_descriptor {
    location      = "${local.transform_root}/gold/zone_hour_earnings/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = local.zone_hour_earnings_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

resource "aws_glue_catalog_table" "tipping_behavior" {
  name          = "tipping_behavior"
  database_name = aws_glue_catalog_database.ny_taxi.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  storage_descriptor {
    location      = "${local.transform_root}/gold/tipping_behavior/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = local.tipping_behavior_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

resource "aws_glue_catalog_table" "dq_scorecard" {
  name          = "dq_scorecard"
  database_name = aws_glue_catalog_database.ny_taxi.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  storage_descriptor {
    location      = "${local.transform_root}/gold/dq_scorecard/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = local.dq_scorecard_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}