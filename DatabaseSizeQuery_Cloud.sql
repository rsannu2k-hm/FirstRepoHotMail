
from pyspark.sql import SparkSession, functions as F
# pyspark.sql.functions.monotonically_increasing_id()

spark = SparkSession.builder.getOrCreate()

df = spark.sql("""
          select "select '" || table_catalog || "." || table_schema || "." || table_name || "', count(1) Cnt from " ||
table_catalog || "." || table_schema||"."||table_name || " Union "
from xliidw_uat_lpl.information_schema.tables
where table_schema = 'staging_xlprof_genius' and table_name not like '%_deleted_rows'
order by table_name
--LIMIT 5
""")

counter = 0
final_qry = []
for row in df.collect():

    final_qry.append(df.collect()[counter][0].split("=", 1))
    counter += 1

sqlstr = "".join([item[0] + ' ' for item in final_qry])

# print("final string ", sqlstr)
# display(df_final[1])

sqlstr = sqlstr.strip().rsplit(None, 1)[0]
# print(sqlstr)

df1 = spark.sql(sqlstr)
display(df1)
--------------------------------------------------------------------------------
DECLARE @sql1 NVARCHAR(MAX);
DECLARE @sql NVARCHAR(MAX);
DECLARE @finalsql NVARCHAR(MAX);

DECLARE TableCursor CURSOR LOCAL FAST_FORWARD FOR
		select 'select ' + '''' + table_schema + '.' + table_name  + '''' + ', count(1) Cnt from ' + table_schema+'.'+table_name + ' Union '
		from information_schema.TABLES
		where TABLE_TYPE like 'BASE%';

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @sql1;

WHILE @@FETCH_STATUS = 0
BEGIN

select @sql =  COALESCE(@sql, '') + @sql1 

--print(@sql)

    FETCH NEXT FROM TableCursor INTO @sql1;
END
--select @sql = N'select ' + '''' + table_schema + '.' + table_name  + '''' + ', count(1) Cnt from ' + table_schema+'.'+table_name + ' Union'
--from information_schema.TABLES
--where --table_schema = 'staging_xlprof_genius' and 
--TABLE_TYPE like 'BASE%';

set @finalsql = left(@sql, len(@sql) - 6)

--print(@sql)

EXEC sp_executesql @finalsql;
--------------------------------------------------------------------------------
dbutils.secrets.listScopes()
dbutils.secrets.list('zxlc0279keycdvue2key02')
--------------------------------------------------------------------------------
DECLARE @tableName NVARCHAR(MAX);
DECLARE @columnName NVARCHAR(MAX);
DECLARE @IsIdentityFlag int;

DECLARE TableCursor CURSOR LOCAL FAST_FORWARD FOR
		select table_name, column_name
		from information_schema.COLUMNS
		--where TABLE_TYPE like 'BASE%';

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @tableName, @columnName;

WHILE @@FETCH_STATUS = 0
BEGIN

--select @sql =  COALESCE(@sql, '') + @sql1 
--print(@sql)

SELECT @IsIdentityFlag = COLUMNPROPERTY(OBJECT_ID(@tableName), @columnName, 'IsIdentity');

if @IsIdentityFlag = 1 
	Begin
		print (@tableName +  ' - '  + @columnName +  ' - ' + convert (varchar, @IsIdentityFlag));
	end;

   FETCH NEXT FROM TableCursor INTO @tableName, @columnName;
END

--------------------------------------------------------------------------------
from pyspark.sql import SparkSession

# Initialize Spark session
# spark = SparkSession.builder.appName("CountTables").getOrCreate()
spark = SparkSession.getActiveSession()

# List of table names in your schema
# You might get this list from a metadata table, or via JDBC, or hardcoded
tables_in_schema = ["zuevdf00", "zugkdf00", "zugpdf00"]  # Replace with your actual table names

df = spark.sql("show tables in xliidw_uat_lpl.us_genius")
df.show()

# Dictionary to store counts
table_counts = {}
your_schema = "xliidw_uat_lpl.us_genius"
for table_name in tables_in_schema:
    # Load each table as a DataFrame
    df = spark.read.table(f"{your_schema}.{table_name}")  # Use correct schema name
    count = df.count()
    table_counts[table_name] = count

# Print results
for table, cnt in table_counts.items():
    print(f"Table {table} has {cnt} rows.")

# Stop Spark session if needed
# spark.stop()

--------------------------------------------------------------------------------
