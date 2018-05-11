import argparse

from pyspark import SparkContext
from pyspark import SQLContext

from bisparktest.rdd_loader import load_rdd, load_to_df


def load_to_rdd(args, sc, sql_context):
    rdd = load_rdd(sc, args.file, args.skip_header)

    print rdd.take(args.num)


def config_parser():
    parser = argparse.ArgumentParser(description='Load CSV file')
    parser.add_argument(
        '--file',
        dest='file',
        help='Path to CSV file',
        required=True
    )

    sub_parsers = parser.add_subparsers()

    rdd_parser = sub_parsers.add_parser('rdd')
    rdd_parser.set_defaults(func=load_to_rdd)
    rdd_parser.add_argument(
        '--num',
        dest='num',
        default=5,
        type=int
    )

    rdd_parser.add_argument(
        '--skip-header',
        dest='skip_header',
        default='y',
        choices=('y','n')
    )

    df_parser = sub_parsers.add_parser('df')
    df_parser.set_defaults(func=load_to_df)

    return parser


if __name__ == '__main__':
    parser = config_parser()
    args = parser.parse_args()

    with SparkContext(appName='Load CSV') as sc:
        sql_context = SQLContext(sc)
        args.func(args, sc, sql_context)
