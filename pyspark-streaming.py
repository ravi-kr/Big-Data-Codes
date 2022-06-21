#Apache Spark Structured Streaming with Pyspark
#id,firstname,age,profession,city,salary
#101,Zsa,39,Musician,Male,5667

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.\
        builder.\
        appName("readfromcsv").\
        master("local[4]").\
        getOrCreate()
        
schema1 = StructType([StructField('id', IntegerType(), True),
                      StructField('name', StringType(), True),
                      StructField('age', IntegerType(), True),
                      StructField('profession', StringType(), True),
                      StructField('city', StringType(), True),
                      StructField('salary', DoubleType(), True)])
                      
customer = spark.readStream.format("csv").schema(schema1).\
                 option("header", True).option("maxFilesPerTrigger", 1).\
                 load(r"C:\Users\ravi\streamdata")
                 
customer.isStreaming

customer.printSchema()

average_salaries = customer.groupBy("profession").\
                            agg((avg("salary").alias("average_salary")), (count("profession").alias("count"))).\
                            sort(desc("average_salary"))
                            
query = average_salaries.writeStream.format("console").outputMode("complete").start()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.\
        builder.\
        appName("windowed_operation").\
        getOrCreate()
        
rawdata = spark.\
          readStream.\
          format("socket").\
          option("host", "localhost").\
          option("port", 9999).\
          option("includeTimestamp". True).\
          load()
          
query = rawdata.select((rawdata.value).alias("product"),(rawdata.timestamp).alias("time")).\
                groupBy(window("time", "1 minute"), "product").\
                count().sort(desc("window"))
                
result = query.writeStream.outputMode("complete").format("console").start(truncate = False)

# nc -l localhost -p 9999

result.stop()

#Apache Spark Discretized Streams (DStreams) with Pyspark

from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from time import sleep

sc = SparkContext(appName = "DStream_QueueStream")
ssc = StreamingContext(sc, 2)

rddQueue = []
for i in range(3):
    rddQueue += [ssc.sparkContext.parallelize([j for j in range(1, 21)], 10)]
    
inputStream = ssc.queueStream(rddQueue)
mappedStream = inputStream.map(lambda x: (x % 10, 1))
reducedStream = mappedStream.reduceByKey(lambda a, b: a+b)
readStream.pprint()

ssc.start()
#sleep(6)
ssc.stop(stopSparkContext = True, stopGraceFully = True)

rddQueue
rddQueue[0].glom().collect()

from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from time import sleep

sc = SparkContext("local[*]", "StreamingExample")
ssc = StreamingContext(sc, 5)
lines = ssc.textFileStream(r'home/data')

words = lines.flatMap(lambda x: x.split(" "))
mapped_words = words.map(lambda x: (x, 1))
reduced_words = mapped_words.reduceByKey(lambda x,y:x+y)
sorted_words = reduced_words.map(lambda x: (x[1], x[0])).transform(lambda x:x.sortByKey(False))
sorted_words.pprint()

ssc.start()
sleep(20)
ssc.stop(stopSparkContext = True, stopGraceFully = True)

from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(master = "local[*]", appName = "WindowWordCount")
ssc = StreamingContext(sc, 1)
ssc.checkpoint(r'C:\Users\ravi\Desktop\databases\checkpoint')

lines = ssc.socketTextStream("localhost", 9999)
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
pairs.window(10, 5).pprint()

ssc.start()
