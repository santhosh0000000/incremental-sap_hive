# incremental-sap_hive
 Python function fetch data that incrementally fetches data from a SAP HANA database using PySpark and stores the data in an ORC format in a Hive table.


Imports necessary modules:

SparkSession from pyspark.sql to create a Spark session.
os to interact with the operating system, in particular to check the existence of a file.
Defines variables:

Database connection details such as host, port, username, password, etc.
Spark session configurations including app name, master URL, memory allocation, etc.
Creates a Spark session:

A new Spark session is created with the specified configurations, including Hive support enabled.
Determines the offset value:

The script reads the offset from a file. If the file does not exist, it starts with an offset of 0. The offset keeps track of the number of rows already fetched.
Fetches data in chunks:

Inside a while loop, data is fetched from the SAP HANA database in chunks of 50,000 rows at a time using a SQL query with LIMIT and OFFSET clauses.
A new DataFrame is created for each chunk of data.
Writes data to a Hive table:

The fetched data is written to a Hive table in ORC format using the saveAsTable method with "append" mode, so that new data is appended to the existing data in the table.
Updates the offset:

After each chunk of data is fetched and saved, the offset is increased by the number of rows fetched and saved back to the file.
Termination:

If no rows are fetched or the number of rows fetched is less than the limit, it indicates that all available rows have been fetched, and the loop breaks, ending the script.
Stops the Spark session:

After exiting the loop, the Spark session is stopped using the stop method.
Main block:

If the script is executed as the main module, it calls the fetch_data_in_chunks function to start the data fetching process.
Security Note
The script contains sensitive information such as the username and password for the database connection. This is a security risk, and it is recommended to use more secure methods to handle sensitive information, such as environment variables or secure vaults.
