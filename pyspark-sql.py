- Connecting with Spark (Our own Spark Session)

#test.py
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("myApp").getOrCreate()
print("Hello World!")

spark-submit test.py

- Reading in Data

titanic = spark.read.format("csv").\
                     option("separator", ",").\
                     option("header", True).\
                     option("inferSchema", True).\
                     load("file:///home/spark/titanic.csv")
                     
titanic.show()
titanic.printSchema()

people = spark.read.format("json").\
                     option("multiline", True).\
                     load("file:///home/spark/example.json")
                     
people.show()
people.printSchema()

data = spark.read.parquet("file:///home/spark/userdata/userdata1.parquet")

data.show()

data_folder = spark.read.parquet("file:///home/spark/userdata/")

data_folder.show()

data.count()
data_folder.count()

sql_df = spark.read.format("jdbc").\
                    option("url", "jdbc:postgresql://localhost:5432/tutorial").\
                    option("dbtable", "dummy").\
                    option("user", "postgres").\
                    opion("password", "Test").\
                    option("driver", "org.postgresql.Driver").\
                    load()
                    
sql_df.show()
sql_df.printSchema()

- Select, Filter, and Sort

titanic.select("Name", "Survived").show()
titanic.select(["Name", "Survived"]).show()

titanic = titanic.drop("Siblings/Spouses Aboard", "Parents/Children Aboard")

import pyspark.sql.functions as f
titanic.filter(f.col("Sex") == "female").show()
titanic.filter((f.col("Sex") == "female") & (f.col("Pclass") == 1)).show()
titanic.filter(((f.col("Sex") == "female") & (f.col("Pclass") == 1)) | ((f.col("Sex") == "male") & (f.col("Pclass") == 3))).show()
titanic.sort("Age").show()
titanic.sort("Age", ascending = False).show()
titanic_sorted = titanic.sort("Pclass", f.desc("Age"))
titanic_sorted.show()

- Aggregation 

titanic.groupBy("Pclass").agg({"Fare":"mean"}).show()
titanic.groupBy("Pclass").agg(f.mean("Fare"), f.max("Fare")).show()
fare_aggr = titanic.groupBy("Pclass", "Sex").agg(f.mean("Fare"), f.max("Fare"), f.mean("Age"), f.max("Age"), f.min("Age"))
fare_aggr.show()
fare_aggr.sort("Pclass", "Sex")
fare_aggr.sort("Pclass", "Sex").show()
fare_aggr = fare_aggr.withColumnRenamed("max(Fare)", "Fare_max")
fare_aggr.show()
titanic.count()
titanic.groupBy("Pclass", "Sex").count().show()
titanic.groupBy("Pclass", "Sex").count().sort("Pclass", "Sex").show()
titanic.select("Pclass").distinct().show()


- Functions

import pyspark.sql.functions as f
titanic.filter(f.col("Sex") == "female").show()
titanic = titanic.withColumn("FirstClass", f.col("Pclass") == 1)
titanic.show()
titanic.withColumn("Ship", f.lit("Titanic")).select("Name", "Ship").show()
titanic.withColumn("One", f.lit("1")).select("Name", "One").show()
titanic = titanic.withColumn("Fare_log", f.log(f.col("Fare")))
titanic.select("Fare", "Fare_log").show()
titanic = titanic.withColumn("Age_Class", f.when(f.col("Age") < 18, f.lit("Minor")).otherwise(f.lit("Adult")))
titanic.select("Age", "Age_Class").show()
titanic = titanic.withColumn("Title", f.regexp_extract(f.col("Name"), r"^\S+", 0))
titanic.select("Name", "Title").show()
titanic = titanic.withColumn("CommonTitle", f.col("Title").isin(["Mr.", "Mrs.", "Miss."]))
titanic.select("Title", "CommonTitle").show()
titanic.withColumn("Name_Lower", f.lower(f.col("Name"))).select("Name", "Name_Lower").show()


- Join, Union, and Pivot 

employees = spark.read.format("csv").\
                       options(separator = ",", header = True, inferSchema = True).\
                       load("file:///home/spark/office/employees.csv")
employees.show()

salaries = spark.read.format("csv").\
                      options(separator = ",", header = True, inferSchema = True).\
                      load("file:///home/spark/office/salaries.csv")
salaries.show()

jobs = spark.read.format("csv").\
                  options(separator = ",", header = True, inferSchema = True).\
                  .load("file:///home/spark/office/jobs.csv")
jobs.show()
                  
employees = employees.join(jobs, "job", "left")
employees.show()

salaries = salaries.join(employees.select("employees", "department"), "employee", "left")
salaries.show()

salaries_department = salaries.groupBy("date", "department").\
agg({"salary":"sum"})
salaries_department.show()
salaries_department.sort("department", "date").show()

salary_recent = salaries.groupBy("employee").agg({"date":"max"})
salary_recent.show()

salary_recent = salaries.join(salary_recent, "employee", "left").\
filter(f.col("date") == f.col("max(date)"))
salary_recent.show()

employees = employees.join(salary_recent.select("employee", "salary"), "employee", "left")
employees.show()

7 - User Defined Functions, Pandas, and Collect 

a = [("A", 100), ("B", 200)]
b = [("C", 300), ("D", 400)]
a = spark.createDataFrame(a, schema = ["string", "number"])
b = spark.createDataFrame(b, schema = ["string", "number"])
a.show()
b.show()
c = a.union(b)
c.show()

salaries.show()

salaries_pivot = salaries.groupBy("employee").\
                          pivot("date").\
                          sum("salary").\
                          fillna(0)
salaries_pivot.show()

import pyspark.sql.types as t
def to_upper(s: str):
    return s.upper()
    
import pyspark.sql.functions as f
udf_to_upper = f.udf(to_upper, t.StringType())
titanic.withColumn("name_upper", udf_to_upper(f.col("name"))).select("name", "name_upper").show()

titanic_pd = titanic.toPandas()
titanic_spark = spark.createDataFrame(titanic_pd)
titanic_spark.show()

titanic_collect = titanic.select("name", "fare").collect()
for x in titanic_collect:
    print("Name: " + x[0])
    print("Fare: " + str(x[1]))
titanic = spark.read.format("csv").\
                     options(separator = ",", header = True, inferSchema = True).\
                     load("file:///home/spark/titanic.csv")
import pyspark.sql.functions as f
titanic = titanic.withColumn("name_upper", f.upper(f.col("name"))).\
withColumn("fare_log", f.log(f.col("fare")))
titanic.select("name_upper", "fare_log").show()


- Writing Data 

titanic.write.format("csv").\
options(separator = ",").\
save("file:///home/spark/titanic_new.csv")

titanic.repartition.\
write.\
format("csv").\
options(separator = ",").\
save("file:///home/spark/titanic_new.csv")

titanic.select("name", "fare").\
write.\
parquet("file:///home/spark/titanic_new.parquet")

titanic.write.format("jdbc").\
              option("url", "jdbc:postgresql://localhost:5432/tutorial").\
              option("dbtable", "titanic").\
              option("user", "postgres").\
              option("password", "test").\
              option("driver", "org.postgresql.Driver").\
              save()
            
