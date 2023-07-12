import pymysql
from dateutil.relativedelta import relativedelta
import datetime,math

# MySQL数据库连接信息
mysql_host = "xxxxx"
mysql_port = 3306
mysql_user = "xxx"
mysql_password = "xxxx"
mysql_database = "xxxx"

# 全局变量：1、表列表；2、可能的时间分区字段；3、是否动态分区：Auto自动判断、Yes动态分区、No不动态分区；4、Auto模式下动态分区的行数判断线
table_list = ["xxxx","xxxx"]
created_time_field = ["created_time","created_at","create_time","create_at"]
dynamic_partition_or_not = "Yes"
partition_row = 1000000
index_cardinality = 10000

# MySQL的数据类型到Starrocks的数据类型映射关系
def map_data_type(data_type):
    if data_type.startswith("tinyint") and 'unsigned' in data_type:
        return data_type.replace('tinyint', 'smallint').replace('unsigned', '')
    elif data_type.startswith("smallint") and 'unsigned' in data_type:
        return data_type.replace('smallint', 'int').replace('unsigned', '')
    elif data_type.startswith("mediumint"):
        return data_type.replace('mediumint', 'int')
    elif data_type.startswith("int") and 'unsigned' in data_type:
        return data_type.replace('int', 'bigint').replace('unsigned', '')
    elif data_type.startswith("bigint") and 'unsigned' in data_type:
        return data_type.replace('bigint', 'largeint').replace('unsigned', '')
    elif data_type.startswith("float") and 'unsigned' in data_type:
        return data_type.replace('float', 'double').replace('unsigned', '')
    elif data_type.startswith("timestamp"):
        return "DATETIME"
    elif data_type.startswith("datetime"):
        return "DATETIME"
    elif data_type.startswith("time"):
        return "TIME"
    elif data_type.startswith("year"):
        return "INT"
    elif data_type.startswith("char"):
        return "STRING"
    elif data_type.startswith("varchar"):
        return "STRING"
    elif data_type.startswith("text"):
        return "STRING"
    elif data_type.startswith("mediumtext"):
        return "STRING"
    elif data_type.startswith("longtext"):
        return "STRING"
    elif data_type.startswith("binary"):
        return "STRING"
    elif data_type.startswith("varbinary"):
        return "STRING"
    elif data_type.startswith("blob"):
        return "STRING"
    elif data_type.startswith("mediumblob"):
        return "STRING"
    elif data_type.startswith("longblob"):
        return "STRING"
    elif data_type.startswith("enum"):
        return "STRING"
    elif data_type.startswith("set"):
        return "STRING"
    return data_type


# 获取MySQL表结构信息
def get_mysql_table_structure(table_name):
    connection = pymysql.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database,
        charset="utf8mb4"
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT TABLE_ROWS,round(data_length/1024/1024/1024, 2) as size FROM information_schema.tables " \
                           f"WHERE table_schema = '{mysql_database}' AND table_name = '{table_name}' LIMIT 1;")
            table = cursor.fetchone()
            
            cursor.execute(f"SELECT column_name, column_type, column_default, is_nullable, COLUMN_COMMENT FROM information_schema.columns " \
                           f"WHERE table_schema = '{mysql_database}' AND table_name = '{table_name}';")
            columns = cursor.fetchall()
            
            cursor.execute(f"SELECT INDEX_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS " \
                           f"WHERE table_schema = '{mysql_database}' AND TABLE_NAME='{table_name}' " \
                            f"AND SEQ_IN_INDEX=1 AND INDEX_NAME = 'PRIMARY'")
            primary_key = ""
            primary_index = cursor.fetchone()
            if primary_index is not None:
                primary_key = primary_index[1]

            cursor.execute(f"SELECT distinct COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS " \
                           f"WHERE table_schema = '{mysql_database}' AND TABLE_NAME = '{table_name}' " \
                           f"AND INDEX_NAME != 'PRIMARY' AND CARDINALITY <= '{index_cardinality}'")
            bitmap_key = ""
            bitmap_key = cursor.fetchall()
            
            if (table[0] < partition_row and dynamic_partition_or_not == 'Auto') or (dynamic_partition_or_not == 'No'):
                dynamic_partition = False
            else:
                dynamic_partition = True

            partition_col = ""
            for column in columns:
                if column[0] in created_time_field:
                    partition_col = column[0]
                    break
            # 如果columns里没有候选的分区字段里的话，不进行动态分区
            if partition_col == "":
                dynamic_partition = False 

            if dynamic_partition:
                cursor.execute(f"SELECT INDEX_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS " \
                               f"WHERE table_schema = '{mysql_database}' AND TABLE_NAME='{table_name}' " \
                               f"AND SEQ_IN_INDEX=1 AND COLUMN_NAME = '{partition_col}' AND SEQ_IN_INDEX = 1;")
                time_index = cursor.fetchone()
                if time_index:
                    cursor.execute(f"SELECT MIN(`{partition_col}`) FROM `{table_name}`")
                    start = cursor.fetchone()[0]
                    cursor.execute(f"SELECT MAX(`{partition_col}`) FROM `{table_name}`")
                    end = cursor.fetchone()[0]
                else:
                    cursor.execute(f"SELECT `{partition_col}` FROM `{table_name}` order by {primary_key} limit 1")
                    start = cursor.fetchone()[0]
                    cursor.execute(f"SELECT `{partition_col}` FROM `{table_name}` order by {primary_key} desc limit 1")
                    end = cursor.fetchone()[0]
                if start == '0000-00-00 00:00:00':
                    dynamic_partition = False
                    start, end = "", ""
            else:
                start, end = "", ""

            cursor.close()
            
            return {
                "table_name": table_name,
                "columns": columns,
                "primary_key": primary_key,
                "bitmap_key": bitmap_key,
                "table_size": table[1],
                "dynamic" : {"dynamic_partition": dynamic_partition, "partition_col": partition_col, "start": start, "end": end}
            }
    finally:
        connection.close()


def generate_starrocks_create_table_sql(table_definition):
    # 生成分区的语句
    if table_definition['dynamic']['dynamic_partition']:
        partition_by = f"PARTITION BY RANGE(`{table_definition['dynamic']['partition_col']}`)\n" \
                       f"(START('{get_partition_start(table_definition['dynamic']['start'])}') "\
                       f"END('{get_partition_end(table_definition['dynamic']['end'])}') EVERY (INTERVAL 1 month))\n"
    else:
        partition_by = ""

    # 生成列字段的语句，注意分区情况下字段的顺序以及字段的映射；增加索引语句的生成
    column_definitions = []
    for column in table_definition["columns"]:
        column_definition = f"`{column[0]}` {map_data_type(column[1])}"
        if column[3] == "NO" and map_data_type(column[1]) != 'DATETIME':
            column_definition += " NOT NULL"
        if column[2] is not None and map_data_type(column[1]) != 'DATETIME':
            column_definition += f" DEFAULT '{column[2]}'"
        column_definition += f" COMMENT '{column[4]}'"
        column_definition += ",\n"
        if table_definition['dynamic']['dynamic_partition'] and column[0] == table_definition['dynamic']['partition_col']:
            column_definitions.insert(1,column_definition)
        else:
            column_definitions.append(column_definition)
    for key in table_definition['bitmap_key']:
        key_definition = f"INDEX idx_{key[0]} (`{key[0]}`) USING BITMAP,\n"
        column_definitions.append(key_definition)
    column_definitions[-1] = column_definitions[-1].replace(',\n', '')

    # 主键的定义
    if table_definition['primary_key'] == '':
        primary_key = ""
    else:
        if table_definition['dynamic']['dynamic_partition']:
            primary_key = f"PRIMARY KEY (`{table_definition['primary_key']}`,`{table_definition['dynamic']['partition_col']}`)"
        else:
            primary_key = f"PRIMARY KEY (`{table_definition['primary_key']}`)"

    # 分桶数计算
    bucket = get_bucket_count(table_definition['table_size'],table_definition['dynamic']['dynamic_partition'],table_definition['dynamic']['start'],table_definition['dynamic']['end'])

    # 表性质的定义，动态分区和非分区表不同
    properties = ""
    if table_definition['dynamic']['dynamic_partition']:
        properties = f"    'replication_num' = '3',\n" \
           f"    'enable_persistent_index' = 'true',\n" \
           f"    'dynamic_partition.enable' = '{str(table_definition['dynamic']['dynamic_partition']).lower()}',\n" \
           f"    'dynamic_partition.time_unit' = 'MONTH',\n" \
           f"    'dynamic_partition.time_zone' = 'Asia/Shanghai',\n" \
           f"    'dynamic_partition.start' = '-2147483648',\n" \
           f"    'dynamic_partition.end' = '3',\n" \
           f"    'dynamic_partition.prefix' = 'p',\n" \
           f"    'dynamic_partition.buckets' = '{bucket}'\n" 
    else:
        properties = f"    'replication_num' = '3',\n" \
           f"    'enable_persistent_index' = 'true'\n" 

    # 返回组合建表语句
    return f"CREATE TABLE `{table_definition['table_name']}` (\n" \
           f"{''.join(column_definitions)}\n" \
           f") ENGINE=olap\n" \
           f"{primary_key}\n" \
           f"{partition_by}" \
           f"DISTRIBUTED BY HASH(`{table_definition['primary_key']}`) BUCKETS {bucket}\n" \
           f"PROPERTIES (\n" \
           f"{properties}" \
           f")"

# 获取起始时间的前一个月的1号作为分区的起点
def get_partition_start(start):
    if start is None:
        start = datetime.datetime.now() - relativedelta(months=1)
    else:
        start = start - relativedelta(months=1)
    return start.replace(day=1).strftime("%Y-%m-%d")

# 获取结束时间的后一个月的1号作为分区的终点
def get_partition_end(end):
    if end is None:
        end = datetime.datetime.now() + relativedelta(months=1)
    else:
        end = end + relativedelta(months=1)
    return end.replace(day=1).strftime("%Y-%m-%d")

def get_bucket_count(table_size,dynamic_partition,start,end):
    if not dynamic_partition:
        return min(16, max(4, math.ceil(table_size/3/1024/1024/1024)))
    else:
        time_diff = relativedelta(start, end)
        months_diff = time_diff.months + time_diff.years * 12 + 1
        estimated_size_per_month = table_size / months_diff
        return min(16, max(2, math.ceil(estimated_size_per_month/3/1024/1024/1024)))

if __name__ == "__main__":
    for table in table_list:
        table_definition = get_mysql_table_structure(table)
        create_sql = generate_starrocks_create_table_sql(table_definition)
        print(create_sql)
