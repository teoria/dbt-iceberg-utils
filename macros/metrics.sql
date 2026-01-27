{% macro default__get_table_metrics_sql(catalog,schema,table_name) %}
{% set date_run = run_started_at.strftime("%Y-%m-%d %H:%M:%S") %}
     
     MERGE INTO {{ catalog }}.{{ schema }}_iceberg."iceberg_metrics" as DBT_INTERNAL_DEST
            USING (
        with
        iceberg_snapshot_metric as (
            SELECT 
                '{{ table_name }}' as table_name,
                cast('{{ date_run }}' AS timestamp(6) WITH time zone)  as created_at,
                summary['changed-partition-count'] as "changed_partition_count", 
                summary['total-equality-deletes'] as "total_equality_deletes", 
                summary['total-position-deletes'] as "total_position_deletes", 
                summary['total-delete-files']  as "total_delete_files",
                summary['total-files-size']  as "total_files_size",
                summary['total-records'] as "total_records", 
                summary['total-data-files']  as "total_data_files"
            FROM  {{ catalog }}.{{ schema }}."{{ table_name }}$snapshots" ORDER BY committed_at DESC LIMIT 1

        )
        , iceberg_files_metric as (

            SELECT 
                '{{ table_name }}' as table_name,
                '{{ date_run }}' as created_at,
                CAST(AVG(record_count) as INT) as avg_record_count, 
                MAX(record_count) as max_record_count, 
                MIN(record_count) as min_record_count, 
                CAST(AVG(file_size_in_bytes) as INT) as avg_file_size, 
                MAX(file_size_in_bytes) as max_file_size, 
                MIN(file_size_in_bytes) as min_file_size 
            FROM {{ catalog }}.{{ schema }}."{{ table_name }}$files"
        )
        , iceberg_partitions_data as (
            SELECT 
                        record_count,
                        file_count,
                        CAST(total_size AS BIGINT) as file_size
            FROM {{ catalog }}.{{ schema }}."{{ table_name }}$partitions"
        )
        ,iceberg_partitions_metric as (
            SELECT 

                '{{ table_name }}' as table_name,
                '{{ date_run }}' as created_at,
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
            iceberg_snapshot_metric.created_at,
            cast(iceberg_snapshot_metric.changed_partition_count as bigint) as changed_partition_count, 
            cast(iceberg_snapshot_metric.total_equality_deletes as bigint) as total_equality_deletes, 
            cast(iceberg_snapshot_metric.total_position_deletes as bigint) as total_position_deletes, 
            cast(iceberg_snapshot_metric.total_delete_files as bigint) as total_delete_files,
            cast(iceberg_snapshot_metric.total_files_size as bigint) as total_files_size,
            cast(iceberg_snapshot_metric.total_records as bigint) as total_records,
            cast(iceberg_snapshot_metric.total_data_files as bigint) as total_data_files,

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
                    DBT_INTERNAL_SOURCE.created_at = DBT_INTERNAL_DEST.created_at
                )
        when matched then update set
            "table_name" = DBT_INTERNAL_SOURCE."table_name","created_at" = DBT_INTERNAL_SOURCE."created_at","changed_partition_count" = DBT_INTERNAL_SOURCE."changed_partition_count","total_equality_deletes" = DBT_INTERNAL_SOURCE."total_equality_deletes","total_position_deletes" = DBT_INTERNAL_SOURCE."total_position_deletes","total_delete_files" = DBT_INTERNAL_SOURCE."total_delete_files","total_files_size" = DBT_INTERNAL_SOURCE."total_files_size","total_records" = DBT_INTERNAL_SOURCE."total_records","total_data_files" = DBT_INTERNAL_SOURCE."total_data_files","total_partitions" = DBT_INTERNAL_SOURCE."total_partitions","avg_partition_record_count" = DBT_INTERNAL_SOURCE."avg_partition_record_count","max_partition_record_count" = DBT_INTERNAL_SOURCE."max_partition_record_count","min_partition_record_count" = DBT_INTERNAL_SOURCE."min_partition_record_count","deviation_record_count" = DBT_INTERNAL_SOURCE."deviation_record_count","avg_file_count" = DBT_INTERNAL_SOURCE."avg_file_count","max_file_count" = DBT_INTERNAL_SOURCE."max_file_count","min_file_count" = DBT_INTERNAL_SOURCE."min_file_count","total_size_bytes" = DBT_INTERNAL_SOURCE."total_size_bytes","avg_file_record_count" = DBT_INTERNAL_SOURCE."avg_file_record_count","max_file_record_count" = DBT_INTERNAL_SOURCE."max_file_record_count","min_file_record_count" = DBT_INTERNAL_SOURCE."min_file_record_count","avg_file_size" = DBT_INTERNAL_SOURCE."avg_file_size","max_file_size" = DBT_INTERNAL_SOURCE."max_file_size","min_file_size" = DBT_INTERNAL_SOURCE."min_file_size"
        when not matched then insert
            ("table_name", "created_at", "changed_partition_count", "total_equality_deletes", "total_position_deletes", "total_delete_files", "total_files_size", "total_records", "total_data_files", "total_partitions", "avg_partition_record_count", "max_partition_record_count", "min_partition_record_count", "deviation_record_count", "avg_file_count", "max_file_count", "min_file_count", "total_size_bytes", "avg_file_record_count", "max_file_record_count", "min_file_record_count", "avg_file_size", "max_file_size", "min_file_size")
        values
            (DBT_INTERNAL_SOURCE."table_name", DBT_INTERNAL_SOURCE."created_at", DBT_INTERNAL_SOURCE."changed_partition_count", DBT_INTERNAL_SOURCE."total_equality_deletes", DBT_INTERNAL_SOURCE."total_position_deletes", DBT_INTERNAL_SOURCE."total_delete_files", DBT_INTERNAL_SOURCE."total_files_size", DBT_INTERNAL_SOURCE."total_records", DBT_INTERNAL_SOURCE."total_data_files", DBT_INTERNAL_SOURCE."total_partitions", DBT_INTERNAL_SOURCE."avg_partition_record_count", DBT_INTERNAL_SOURCE."max_partition_record_count", DBT_INTERNAL_SOURCE."min_partition_record_count", DBT_INTERNAL_SOURCE."deviation_record_count", DBT_INTERNAL_SOURCE."avg_file_count", DBT_INTERNAL_SOURCE."max_file_count", DBT_INTERNAL_SOURCE."min_file_count", DBT_INTERNAL_SOURCE."total_size_bytes", DBT_INTERNAL_SOURCE."avg_file_record_count", DBT_INTERNAL_SOURCE."max_file_record_count", DBT_INTERNAL_SOURCE."min_file_record_count", DBT_INTERNAL_SOURCE."avg_file_size", DBT_INTERNAL_SOURCE."max_file_size", DBT_INTERNAL_SOURCE."min_file_size")

{% endmacro %}


{% macro run_get_table_metrics(table_name) %} 
    
    {{ log( "<<<<<<<<antes>>>>>>>" , True) }} 
    {{ log(table_name, True) }} 
    {% set query_files = get_table_metrics_sql( table_name ) %} 
    {{ log( "<<<<<<<<fim>>>>>>>" , True) }}  
    
    -- {{ log( query_files , True) }}
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
  
SELECT
	table_name,
    created_at,
    -- COUNT(*) as total_files2,
    -- SUM(total_size_bytes) / 1024 / 1024 as total_size_mb2,
    -- AVG(total_size_bytes) / 1024 / 1024 as avg_file_size_mb2,
    -- MIN(total_size_bytes) / 1024 / 1024 as min_file_size_mb2,
    -- MAX(total_size_bytes) / 1024 / 1024 as max_file_size_mb2,
     ROUND((total_position_deletes + total_equality_deletes) * 100.0 / NULLIF( (total_records), 0), 2) AS delete_ratio_percent,
  ROUND((total_position_deletes + total_equality_deletes) * 100.0 / NULLIF( (total_data_files), 0), 2) AS delete_file_ratio_percent,
  -- extra ratios
  ROUND( (total_records) * 1.0 / NULLIF( (total_data_files), 0), 2) AS avg_records_per_file,
  ROUND((total_position_deletes + total_equality_deletes) * 1.0 / NULLIF((total_position_deletes + total_equality_deletes), 0), 2) AS avg_deletes_per_delete_file ,
   total_records,
  total_position_deletes, 
  total_equality_deletes,
  total_position_deletes + total_equality_deletes AS total_deletes,
  total_data_files AS total_files_count,
  ROUND(coalesce(total_size_bytes,0) / 1048576.0, 2) AS total_size_mb,
  ROUND(coalesce(avg_file_size,0) / 1048576.0, 2) AS avg_file_size_mb,
  total_delete_files  
from iceberg.jaffle_shop_iceberg.iceberg_metrics
order by table_name,created_at desc
 

  {% endset %}
    {% set result = run_query(query) %}
    {%- if execute %} 
   {{ log(result.print_table(),info=TRUE) }}
    {{ return(result.columns[0].values()) }}
  {% endif %}
{% endmacro %}



{% macro get_iceberg_tables_needing_maintenance(
    delete_file_ratio_threshold=10,
    total_delete_files_threshold=100,
    deviation_record_count_threshold=100000,
    min_file_size_mb_threshold=1,
    limit_size=None
) %} 
    {#
        Criteria (customize as needed):
        - delete_file_ratio_percent > delete_file_ratio_threshold
        - total_delete_files > total_delete_files_threshold
        - deviation_record_count > deviation_record_count_threshold
        - min_file_size_mb < min_file_size_mb_threshold
    #}
    {% set query %}
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

        FROM {{ database }}.{{ schema }}_iceberg.iceberg_metrics
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
    SELECT table_name, why
    FROM tables_with_issues
    where why is not null
    {% if limit_size is not none %}
    LIMIT {{ limit_size }}
        {% endif %}
    {% endset %}

    {{ log(query, info=False) }}

    {% set result = run_query(query) %}
    {%- if execute %}  
        {{ return( result ) }}
    {% endif %}
{% endmacro %}
