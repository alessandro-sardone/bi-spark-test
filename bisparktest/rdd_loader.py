from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType


def parse_row(row):
    return int(row[0]), row[1], row[2], row[3]


def load_rdd(sc, file_path, skip_header):
    rdd = sc.textFile('file://{}'.format(file_path))

    # returns list of lists, split by comma
    rdd = rdd.map(lambda x: x.split(','))

    if skip_header == 'y':
        header = rdd.first()
        rdd = rdd.filter(lambda x: x != header)
        # Return everything that isnt the header
        rdd = rdd.map(parse_row)

    return rdd


def load_to_df(args, sc, sql_context):
    rdd = load_rdd(sc, args.file, 'y')

    # Create schema
    schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('first_name', StringType(), True),
        StructField('last_name', StringType(), True),
        StructField('email', StringType(), True)
    ])

    df = sql_context.createDataFrame(rdd, schema)

    df.show()


    #df.registerTempTable('df')

    #rdd = sql_context.sql(
    #    '''
     #   SELECT count(*) as c from df
     ##   '''
    #

   # print rdd.first()