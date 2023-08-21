from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# sparkSession
spark = SparkSession.builder.appName("broadcastJoin").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Creating large dataframe
largeDF = spark.read.option("inferschema", "true").csv("/home/xs354-riygup/TASKS/kafka/data/sparkData.csv",
                                                       header=True)
print("\n Larger Dataframe:")
largeDF.show()

# creating smaller dataframe
data = [(1, "BMW"),
        (2, "Hyundai"),
        (3, "Tata"),
        (33, "Kia")]

smallerDF = spark.createDataFrame(data, ["id", "car"])

print('\n smaller Dataframe:')
smallerDF.show()

broadcastJoin = largeDF.join(broadcast(smallerDF), smallerDF.id == largeDF.RecordNumber)
# physical plan of broadcast join .explain(extended=False)

print('BroadcastJoin: ')
broadcastJoin.show()

