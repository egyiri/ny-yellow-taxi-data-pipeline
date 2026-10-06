output "redshift_workgroup_endpoint" {
  value = aws_redshiftserverless_workgroup.ny_taxi.endpoint
}

output "glue_database_name" {
  value = aws_glue_catalog_database.ny_taxi.name
}

output "glue_clean_trips_table_name" {
  value = aws_glue_catalog_table.clean_trips.name
}
