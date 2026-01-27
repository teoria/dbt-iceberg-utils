
{{
  config(
    materialized='view',
    enabled=true  
  )
}}
{% set delete_file_ratio_threshold=10 %}
{% set total_delete_files_threshold=100 %}
{% set deviation_record_count_threshold=100000 %}
{% set min_file_size_mb_threshold=1 %}  

WITH iceberg_metrics AS (
        SELECT
            table_name,
            created_at,
            total_position_deletes,
            total_equality_deletes,
            total_delete_files,
            deviation_record_count,
            min_file_size,
            total_data_files,
            total_records,
            total_size_bytes,
            ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY created_at DESC) AS rn

        FROM {{ database }}.{{ schema }}.iceberg_metrics
    ),
    filtered_metrics AS (
        SELECT *
        FROM iceberg_metrics
        WHERE rn = 1
    ),
    tables_with_issues AS (

    SELECT
        table_name,
        created_at,
        (
            CASE
                WHEN ((total_position_deletes + total_equality_deletes) * 100.0 / NULLIF((total_data_files), 0)) > {{ delete_file_ratio_threshold }} THEN 'High delete_file_ratio_percent'
                ELSE NULL
            END
        ) ||
        (
            CASE
                WHEN total_delete_files > {{ total_delete_files_threshold }} THEN
                    CASE WHEN ((total_position_deletes + total_equality_deletes) * 100.0 / NULLIF((total_data_files), 0)) > {{ delete_file_ratio_threshold }} THEN ', ' ELSE '' END || 'High total_delete_files'
                ELSE ''
            END
        ) ||
        (
            CASE
                WHEN deviation_record_count > {{ deviation_record_count_threshold }} THEN
                    CASE WHEN ((total_position_deletes + total_equality_deletes) * 100.0 / NULLIF((total_data_files), 0)) > {{ delete_file_ratio_threshold }} OR total_delete_files > {{ total_delete_files_threshold }} THEN ', ' ELSE '' END || 'High deviation_record_count'
                ELSE ''
            END
        ) ||
        (
            CASE
                WHEN (coalesce(min_file_size,0) / 1048576.0) < {{ min_file_size_mb_threshold }} THEN
                    CASE WHEN ((total_position_deletes + total_equality_deletes) * 100.0 / NULLIF((total_data_files), 0)) > {{ delete_file_ratio_threshold }} OR total_delete_files > {{ total_delete_files_threshold }} OR deviation_record_count > {{ deviation_record_count_threshold }} THEN ', ' ELSE '' END || 'Low min_file_size_mb'
                ELSE ''
            END
        ) AS why,
        ROUND((total_position_deletes + total_equality_deletes) * 100.0 / NULLIF((total_data_files), 0), 2) AS delete_file_ratio_percent,
        total_delete_files,
        deviation_record_count,
        ROUND(coalesce(min_file_size,0) / 1048576.0, 2) AS min_file_size_mb,
        total_data_files,
        total_records,
        total_size_bytes
        
    FROM filtered_metrics
    WHERE
        ((total_position_deletes + total_equality_deletes) * 100.0 / NULLIF((total_data_files), 0)) > {{ delete_file_ratio_threshold }}
        OR total_delete_files > {{ total_delete_files_threshold }}
        OR deviation_record_count > {{ deviation_record_count_threshold }}
        OR (coalesce(min_file_size,0) / 1048576.0) < {{ min_file_size_mb_threshold }}
    ORDER BY table_name, created_at DESC
    )
    SELECT *
    FROM tables_with_issues
    where why is not null