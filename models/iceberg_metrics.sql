
{% if adapter.type() == 'athena' %}

{{
  config(
    materialized='incremental',  
    unique_key=['table_name','created_at'],
    on_schema_change='sync_all_columns',  
    incremental_strategy='merge',
    table_type='iceberg',
    write_compression='snappy',
    format='parquet',
    unique_tmp_table_suffix=True,
    table_properties={ 
        'vacuum_max_snapshot_age_seconds': '295200'
    }
  )
}}
{% else %}

{{
  config(
    materialized='incremental',  
    unique_key=['table_name','created_at'],
    on_schema_change='sync_all_columns',  
    incremental_strategy='merge'
  )
}}
{% endif %}


SELECT
    'name' AS table_name,
    cast('2026-01-05 02:48:06' AS timestamp(6) with time zone) AS created_at,
    9999999999999 AS changed_partition_count,
    9999999999999 AS total_equality_deletes,
    9999999999999 AS total_position_deletes,
    9999999999999 AS total_delete_files,
    9999999999999 AS total_files_size,
    9999999999999 AS total_records,
    9999999999999 AS total_data_files,

    9999999999999 AS total_partitions,
    9999999999999 AS avg_partition_record_count,
    9999999999999 AS max_partition_record_count,
    9999999999999 AS min_partition_record_count,
    9999999999999 AS deviation_record_count,
    9999999999999 AS avg_file_count,
    9999999999999 AS max_file_count,
    9999999999999 AS min_file_count,
    9999999999999 AS total_size_bytes,

    9999999999999 AS avg_file_record_count,
    9999999999999 AS max_file_record_count,
    9999999999999 AS min_file_record_count,
    9999999999999 AS avg_file_size,
    9999999999999 AS max_file_size,
    9999999999999 AS min_file_size

WHERE 1 = 0
