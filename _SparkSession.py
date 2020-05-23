from pyspark.sql import SparkSession
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType

spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .getOrCreate()
    
    
spark.sql("SHOW databases").show()
  
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
 
