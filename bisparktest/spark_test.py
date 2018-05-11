from pyspark import SparkContext
# For making dataframes
from pyspark import SQLContext
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType

if __name__ == '__main__':

    # Use with so it closes after execution
    with(SparkContext(appName='My Spark Application')) as sc:

        print 'It works!'

        sql_context = SQLContext(sc)

        # Allows it to work in parallel
        rdd = sc.parallelize([
            ('john', 1),
            ('tori', 2),
            ('alex', 3),
            ('julia', 4),
            ('chris', 5)
        ])

        # Combines keys together and add them up
        rdd = rdd.reduceByKey(lambda a, b: a + b)

        # Make table schema with types
        schema = StructType([
            StructField('name', StringType()),
            StructField('price', IntegerType())
        ])

        # Pass RDD with data and schema
        df = sql_context.createDataFrame(rdd, schema)

        # print rdd.take(5)

        df.show()
        df.printSchema()