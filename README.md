## Goal
This script is to generate the DDL for StarRocks according to definition of tables in mysql, which is the first step to do synchronization of data in real time from mysql to starrocks.

## Usage
You can modify the first few lines of the script to customize according to your needs, because there is no configuration file yet.
For example, you can modify ```dynamic_partition_or_not``` to define whether to do dynamic partition.
```
table_list = ["xxxx","xxxx"]  ## list of tables that need to do ddl convertion
created_time_field = ["created_time","created_at","create_time","create_at"] ## Possible partition column fields
dynamic_partition_or_not = "Yes"  ## whether to do dynamic partition. Yes: do dynamic partition; No: not do dynamic partition; Auto: Determine according to whether the number of rows from the statistic information of mysql is greater than "partition_row" or not
partition_row = 1000000
index_cardinality = 400  ## Determine whether to create bitmap index according to the index cardinality of column fields
```

## Dependencies Installation
```
pip3 install dateutil
pip3 install pymysql
```

## Data type conversion
|        MySQL      | StarRocks |
| ----------------- | --------- |
| BOOLEAN           | BOOLEAN   |
| TINYINT           | TINYINT   |
| TINYINT UNSIGNED  | SMALLINT  |
| SMALLINT          | SMALLINT  |
| SMALLINT UNSIGNED | INT       |
| MEDIUMINTINT      | INT       |
| BIGINT            | BIGINT    |
| BIGINT UNSIGNED   | LARGEINT  |
| FLOAT             | FLOAT     |
| FLOAT UNSIGNED    | DOUBLE    |
| DOUBLE            | DOUBLE    |
| DECIMAL           | DECIMAL   |
| CHAR              | STRING    |
| VARCHAR           | STRING    |
| TEXT              | STRING    |
| DATE              | DATE      |
| DATETIME          | DATETIME  |
| TIMESTAMP         | DATETIME  |
| TIME              | TIME      |