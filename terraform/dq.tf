resource "aws_glue_data_quality_ruleset" "ny_yellow_taxi" {
  name        = "ny_yellow_taxi_data_quality_ruleset"
  description = "Quality checks for the clean_trips silver table"

  target_table {
    database_name = aws_glue_catalog_database.ny_taxi.name
    table_name    = aws_glue_catalog_table.clean_trips.name
  }

  ruleset = "Rules = [ ColumnValues \"fare_amount\" >= 0, ColumnValues \"total_amount\" >= 0, ColumnValues \"trip_distance\" >= 0, IsComplete \"pulocationid\", IsComplete \"dolocationid\", (ColumnValues \"passenger_count\" >= 1) and (ColumnValues \"passenger_count\" <= 6) ]"
}