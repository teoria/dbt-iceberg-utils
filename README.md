# dbt_iceberg_utils

Utility package for projects using [dbt](https://www.getdbt.com/) and [iceberg](https://duckdb.org/) with the adapter [dbt-trino](https://github.com/starburstdata/dbt-trino), [dbt-spark](https://github.com/dbt-labs/dbt-spark) or [dbt-athena](https://github.com/dbt-labs/dbt-athena).

## Installation

1. Add to your packages.yml:

```yaml
packages:
  - package: teoria/dbt_iceberg_utils
    version: 1.1.0 # or any other version
```

2. Run `dbt deps`
3. Add to your `dbt_project.yml`:

```yaml
models:
  ## dbt_iceberg_utils models will be created in the schema '<your_schema>_iceberg'

  iceberg_utils:
    +schema: "iceberg"

```

4. Run `dbt run --select iceberg_utils`

5. Call the macro for collect metrics on the `dbt_project.yml`
```yaml
models:
  iceberg_utils:
    +schema: "iceberg"
  my_project:
    +post-hook: "{{ iceberg_utils.get_table_metrics_sql(this)}}"
```
or 

at model config:
```yaml
{{ config( 
    materialized='incremental',
    unique_key = 'customer_id', 
    incremental_strategy = 'merge',
    table_type='iceberg',
    post_hook="{{ iceberg_utils.get_table_metrics_sql(this) }}"
   )
}}
```

or call the macro directly
```bash
dbt run-operation iceberg_utils.run_get_table_metrics --args '{"table_name":"customers"}'
```
6 . Add the on-run-end hook to your dbt_project.yml:

```bash
on-run-end:
  - "{{ iceberg_utils.on_run_end() }}"
```
After running the `dbt run` command, Iceberg Utils will display a suggestion of 50 tables that need maintenance.

```yaml

      ___         _                      _   _ _   _ _       
     |_ _|___ ___| |__   ___ _ __ __ _  | | | | |_(_) |___   
      | |/ __/ _ \ '_ \ / _ \ '__/ _` | | | | | __| | / __|   
      | | (_|  __/ |_) |  __/ | | (_| | | |_| | |_| | \__ \   
     |___\___\___|_.__/ \___|_|  \__, |  \___/ \__|_|_|___/    
                                 |___/                          
  
Iceberg tables that need maintenance: (1)
-------------------------------------
╔-------------------------------------------------------------------╗
│ Table Name │                         Why?                         │
│------------|------------------------------------------------------|
│  customers │ High delete_file_ratio_percent, Low min_file_size_mb │
╚-------------------------------------------------------------------╝
 
 
Iceberg Table Maintenance:
 
Run the 'iceberg_utils.run_optimize' macro and 'iceberg_utils.run_vacuum' to optimize and vacuum these tables.
 
Usage example:
 
 dbt run-operation run_optimize --args 'table_name: customers'
 dbt run-operation run_vacuum --args 'table_name: customers'
 
¸,ø¤º°`°º¤ø¤º°`°º¤ø,¸¸,ø¤º°`°º¤ø¤º°`°º¤ø,¸¸,ø¤º°`°º¤ø¤º°`°º¤ø,¸
```

## License

MIT license