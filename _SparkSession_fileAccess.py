from pyspark.sql import SparkSession
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType

spark = SparkSession.builder \
     .master("local[*]") \
   .config("spark.hadoop.fs.s3a.s3guard.ddb.region","us-east-2") \
   .config("spark.yarn.access.hadoopFileSystems","s3a://cdp-sandbox-default-se") \
    .config("spark.sql.warehouse.dir", "s3a://cdp-sandbox-default-se") \
    .config("spark.hadoop.yarn.resourcemanager.principal", "skiaie") \
   .getOrCreate()  
    
    
sql0 = """
drop table students
"""   


sql1 = """
CREATE EXTERNAL TABLE IF NOT EXISTS students(student_id INT)
COMMENT 'Student Names'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3a://cdp-sandbox-default-se/sbk-temp/students'
"""    

sql2 = """
CREATE TABLE IF NOT EXISTS 
students(student_id INT)
 """

sql3 = """
alter table students ADD columns (dept String)
"""

sql4 = """
insert into students values (0, "math") 
"""

sql5 = """
select * from students 
"""

spark.sql(sql0).show()
 
spark.sql(sql1).show()
 
spark.sql(sql3).show()

spark.sql(sql4).show()
 
spark.sql(sql5).show()
 
