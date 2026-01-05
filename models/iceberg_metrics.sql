{{
  config(
    materialized='incremental',  
    unique_key=['table_name','safra'],
    on_schema_change='sync_all_columns',  
    incremental_strategy='merge'
  )
}}

SELECT
    'name' AS table_name,
    cast('2026-01-05 02:48:06' AS timestamp(6) with time zone) AS safra,
    0 AS changed_partition_count,
    0 AS total_equality_deletes,
    0 AS total_position_deletes,
    0 AS total_delete_files,
    0 AS total_files_size,
    0 AS total_records,
    0 AS total_data_files,

    0 AS total_partitions,
    0 AS avg_partition_record_count,
    0 AS max_partition_record_count,
    0 AS min_partition_record_count,
    0 AS deviation_record_count,
    0 AS avg_file_count,
    0 AS max_file_count,
    0 AS min_file_count,
    0 AS total_size_bytes,

    0 AS avg_file_record_count,
    0 AS max_file_record_count,
    0 AS min_file_record_count,
    0 AS avg_file_size,
    0 AS max_file_size,
    0 AS min_file_size

WHERE 1 = 0
