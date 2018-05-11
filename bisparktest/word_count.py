from pyspark import SparkContext


if __name__ == '__main__':
    with SparkContext(appName='Word Count') as sc:
        rdd = sc.textFile('file:///hellofresh/data/words.txt')

        rdd = rdd.map(lambda x: (x, 1))
        rdd = rdd.reduceByKey(lambda x, y: x + y)

        # Coalesce function will specify partition #
        print 'Saving results...'
        rdd.coalesce(1).saveAsTextFile('file:///hellofresh/data/wc_results.txt')

        print rdd.take(10)
