from pyspark.sql import SparkSession
import os

def fetch_data_in_chunks():
    host = "modl.geepas.local"
    port = "3453"
    username = "DemoUser"
    password = "DemoPw"
    url = f"jdbc:sap://{host}:{port}"
    schema = "TEST)!"
    table_name = "NDRF"
    hive_dbname = "TESTDB"


    spark = SparkSession.builder \
        .appName("SparkConnector") \
        .master("local[*]") \
        .config("spark.executor.memory", "16g") \
        .config("spark.executor.cores", "8") \
        .config("spark.jars", "/root/ngdbc-2.17.12.jar") \
        .config("spark.cores.max", "192") \
        .config("hive.metastore.uris", "thrift://hdp01-preprod.geepas.local:9083") \
        .config("hive.metastore.client.capability.check", "false") \
        .enableHiveSupport() \
        .getOrCreate()

    limit = 50000
    offset_file_path = '/root/offset_file.txt'

    # Read the offset from the file if it exists, otherwise start from 0
    if os.path.exists(offset_file_path):
        with open(offset_file_path, 'r') as file:
            offset = int(file.read().strip())
    else:
        offset = 0

    while True:
        query = f"(SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset}) AS tmp"
        df = spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("user", username) \
            .option("password", password) \
            .option("dbtable", query) \
            .option("currentSchema", schema) \
            .option("driver", "com.sap.db.jdbc.Driver") \
            .load()

        count = df.count()
        # if count == 0:
        #     break

        print(f"Fetched {count} rows.")


        df.write.format("orc").option("path", "/demo").mode("append").saveAsTable(f"{hive_dbname}.{table_name}")

        # Adding the fetched count to the offset
        offset += count

        # Write the new offset value back to the file
        with open(offset_file_path, 'w') as file:
            file.write(str(offset))

        # Break the loop if the count is less than the limit,
        # indicating that we have fetched all available rows
        if count < limit:
            break

    spark.stop()

if __name__ == "__main__":
    fetch_data_in_chunks()
