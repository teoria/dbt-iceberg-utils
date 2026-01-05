
{% macro get_table_metrics(catalog_name, schema_name, table_name) %} 
    {% set date_run = run_started_at.strftime("%Y-%m-%d %H:%M:%S") %}
    {% set query_files %}
     MERGE INTO "iceberg"."jaffle_shop_iceberg"."iceberg_metrics" as DBT_INTERNAL_DEST
            USING (
        with
        iceberg_snapshot_metric as (
            SELECT 
                '{{ table_name }}' as table_name,
                cast('{{ date_run }}' AS timestamp(6) WITH time zone)  as safra,
                summary['changed-partition-count'] as "changed_partition_count", 
                summary['total-equality-deletes'] as "total_equality_deletes", 
                summary['total-position-deletes'] as "total_position_deletes", 
                summary['total-delete-files']  as "total_delete_files",
                summary['total-files-size']  as "total_files_size",
                summary['total-records'] as "total_records", 
                summary['total-data-files']  as "total_data_files"
            FROM {{ catalog_name }}.{{ schema_name }}."{{ table_name }}$snapshots" ORDER BY committed_at DESC LIMIT 1

        )
        , iceberg_files_metric as (

            SELECT 
                '{{ table_name }}' as table_name,
                '{{ date_run }}' as safra,
                CAST(AVG(record_count) as INT) as avg_record_count, 
                MAX(record_count) as max_record_count, 
                MIN(record_count) as min_record_count, 
                CAST(AVG(file_size_in_bytes) as INT) as avg_file_size, 
                MAX(file_size_in_bytes) as max_file_size, 
                MIN(file_size_in_bytes) as min_file_size 
            FROM {{ catalog_name }}.{{ schema_name }}."{{ table_name }}$files"
        )
        , iceberg_partitions_data as (
            SELECT 
                        record_count,
                        file_count,
                        CAST(total_size AS BIGINT) as file_size
            FROM {{ catalog_name }}.{{ schema_name }}."{{ table_name }}$partitions"
        )
        ,iceberg_partitions_metric as (
            SELECT 

                '{{ table_name }}' as table_name,
                '{{ date_run }}' as safra,
                count(*) as total_partitions,
                avg(record_count) avg_record_count,
                max(record_count) max_record_count,
                min(record_count) min_record_count,
                stddev_pop(record_count) deviation_record_count,
                avg(file_count) avg_file_count,
                max(file_count) max_file_count,
                min(record_count) min_file_count,
                sum(file_size) total_size_bytes 
            from iceberg_partitions_data
        )
         
            SELECT  

            iceberg_snapshot_metric.table_name,
            iceberg_snapshot_metric.safra,
            cast(iceberg_snapshot_metric.changed_partition_count as int) as changed_partition_count, 
            cast(iceberg_snapshot_metric.total_equality_deletes as int) as total_equality_deletes, 
            cast(iceberg_snapshot_metric.total_position_deletes as int) as total_position_deletes, 
            cast(iceberg_snapshot_metric.total_delete_files as int) as total_delete_files,
            cast(iceberg_snapshot_metric.total_files_size as int) as total_files_size,
            cast(iceberg_snapshot_metric.total_records as int) as total_records,
            cast(iceberg_snapshot_metric.total_data_files as int) as total_data_files,

            iceberg_partitions_metric.total_partitions,
            iceberg_partitions_metric.avg_record_count as avg_partition_record_count,
            iceberg_partitions_metric.max_record_count as max_partition_record_count,
            iceberg_partitions_metric.min_record_count as min_partition_record_count,
            iceberg_partitions_metric.deviation_record_count,
            iceberg_partitions_metric.avg_file_count,
            iceberg_partitions_metric.max_file_count,
            iceberg_partitions_metric.min_file_count,
            iceberg_partitions_metric.total_size_bytes  ,

            iceberg_files_metric.avg_record_count as avg_file_record_count, 
            iceberg_files_metric.max_record_count as max_file_record_count, 
            iceberg_files_metric.min_record_count as min_file_record_count, 
            iceberg_files_metric.avg_file_size, 
            iceberg_files_metric.max_file_size, 
            iceberg_files_metric.min_file_size 

        FROM iceberg_snapshot_metric
        LEFT JOIN iceberg_files_metric 
            on iceberg_snapshot_metric.table_name = iceberg_files_metric.table_name
        LEFT JOIN iceberg_partitions_metric 
            on iceberg_snapshot_metric.table_name = iceberg_partitions_metric.table_name
        )
        as DBT_INTERNAL_SOURCE
            on (
                    DBT_INTERNAL_SOURCE.table_name = DBT_INTERNAL_DEST.table_name
                  and  
                    DBT_INTERNAL_SOURCE.safra = DBT_INTERNAL_DEST.safra
                )
        when matched then update set
            "table_name" = DBT_INTERNAL_SOURCE."table_name","safra" = DBT_INTERNAL_SOURCE."safra","changed_partition_count" = DBT_INTERNAL_SOURCE."changed_partition_count","total_equality_deletes" = DBT_INTERNAL_SOURCE."total_equality_deletes","total_position_deletes" = DBT_INTERNAL_SOURCE."total_position_deletes","total_delete_files" = DBT_INTERNAL_SOURCE."total_delete_files","total_files_size" = DBT_INTERNAL_SOURCE."total_files_size","total_records" = DBT_INTERNAL_SOURCE."total_records","total_data_files" = DBT_INTERNAL_SOURCE."total_data_files","total_partitions" = DBT_INTERNAL_SOURCE."total_partitions","avg_partition_record_count" = DBT_INTERNAL_SOURCE."avg_partition_record_count","max_partition_record_count" = DBT_INTERNAL_SOURCE."max_partition_record_count","min_partition_record_count" = DBT_INTERNAL_SOURCE."min_partition_record_count","deviation_record_count" = DBT_INTERNAL_SOURCE."deviation_record_count","avg_file_count" = DBT_INTERNAL_SOURCE."avg_file_count","max_file_count" = DBT_INTERNAL_SOURCE."max_file_count","min_file_count" = DBT_INTERNAL_SOURCE."min_file_count","total_size_bytes" = DBT_INTERNAL_SOURCE."total_size_bytes","avg_file_record_count" = DBT_INTERNAL_SOURCE."avg_file_record_count","max_file_record_count" = DBT_INTERNAL_SOURCE."max_file_record_count","min_file_record_count" = DBT_INTERNAL_SOURCE."min_file_record_count","avg_file_size" = DBT_INTERNAL_SOURCE."avg_file_size","max_file_size" = DBT_INTERNAL_SOURCE."max_file_size","min_file_size" = DBT_INTERNAL_SOURCE."min_file_size"
        when not matched then insert
            ("table_name", "safra", "changed_partition_count", "total_equality_deletes", "total_position_deletes", "total_delete_files", "total_files_size", "total_records", "total_data_files", "total_partitions", "avg_partition_record_count", "max_partition_record_count", "min_partition_record_count", "deviation_record_count", "avg_file_count", "max_file_count", "min_file_count", "total_size_bytes", "avg_file_record_count", "max_file_record_count", "min_file_record_count", "avg_file_size", "max_file_size", "min_file_size")
        values
            (DBT_INTERNAL_SOURCE."table_name", DBT_INTERNAL_SOURCE."safra", DBT_INTERNAL_SOURCE."changed_partition_count", DBT_INTERNAL_SOURCE."total_equality_deletes", DBT_INTERNAL_SOURCE."total_position_deletes", DBT_INTERNAL_SOURCE."total_delete_files", DBT_INTERNAL_SOURCE."total_files_size", DBT_INTERNAL_SOURCE."total_records", DBT_INTERNAL_SOURCE."total_data_files", DBT_INTERNAL_SOURCE."total_partitions", DBT_INTERNAL_SOURCE."avg_partition_record_count", DBT_INTERNAL_SOURCE."max_partition_record_count", DBT_INTERNAL_SOURCE."min_partition_record_count", DBT_INTERNAL_SOURCE."deviation_record_count", DBT_INTERNAL_SOURCE."avg_file_count", DBT_INTERNAL_SOURCE."max_file_count", DBT_INTERNAL_SOURCE."min_file_count", DBT_INTERNAL_SOURCE."total_size_bytes", DBT_INTERNAL_SOURCE."avg_file_record_count", DBT_INTERNAL_SOURCE."max_file_record_count", DBT_INTERNAL_SOURCE."min_file_record_count", DBT_INTERNAL_SOURCE."avg_file_size", DBT_INTERNAL_SOURCE."max_file_size", DBT_INTERNAL_SOURCE."min_file_size")

    {% endset %} 
    {% set results = run_query(query_files) %}

    {% if execute %}
        {% if results is not none %}
            {{ log(results.print_table(), info=True) }}
        {# {{return(results) }} #}
        {% endif %}
    {% endif %}
 
{% endmacro %}

 


{% macro get_iceberg_models() %}
    {% set query %}
with
newest_data as (
  SELECT *, row_number() over( PARTITION BY table_name ORDER BY safra desc ) nr FROM iceberg.jaffle_shop_iceberg.iceberg_metrics
)
SELECT
	table_name,
   total_records,
  total_position_deletes, 
  total_equality_deletes,
  total_position_deletes + total_equality_deletes AS total_deletes,
  total_data_files AS total_files_count,
  ROUND(total_size_bytes / 1048576.0, 2) AS total_size_mb,
  ROUND(avg_file_size / 1048576.0, 2) AS avg_file_size_mb,
  total_delete_files,
  -- % calculations
  ROUND((total_position_deletes + total_equality_deletes) * 100.0 / NULLIF( (total_records), 0), 2) AS delete_ratio_percent,
  ROUND((total_position_deletes + total_equality_deletes) * 100.0 / NULLIF( (total_data_files), 0), 2) AS delete_file_ratio_percent,
  -- extra ratios
  ROUND( (total_records) * 1.0 / NULLIF( (total_data_files), 0), 2) AS avg_records_per_file,
  ROUND((total_position_deletes + total_equality_deletes) * 1.0 / NULLIF((total_position_deletes + total_equality_deletes), 0), 2) AS avg_deletes_per_delete_file 
  from newest_data
where nr = 1
and total_records / total_data_files < 100
LIMIT 100

  {% endset %}
    {% set result = run_query(query) %}
    {%- if execute %} 
   {{ log(result.print_table(),info=TRUE) }}
    {{ return(result.columns[0].values()) }}
  {% endif %}
{% endmacro %}
