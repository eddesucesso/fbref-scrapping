"""
Entrypoint module, in case you use `python -mqr_do_fut`.


Why does this file exist, and why __main__? For more info, read:

- https://www.python.org/dev/peps/pep-0338/
- https://docs.python.org/2/using/cmdline.html#cmdoption-m
- https://docs.python.org/3/using/cmdline.html#cmdoption-m
"""

from cli import ExtractData
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # URL to scrape
    spark = SparkSession.builder.appName('Webscrapping').getOrCreate()

    # URL to scrape
    years = ['2023', '2022', '2021', '2020', '2019', '2018']
    all_data = []
    extract = ExtractData(years, spark)
    extract.extract()
