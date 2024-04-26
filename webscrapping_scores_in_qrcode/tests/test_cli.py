import pytest
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import SparkSession
from src.cli import ExtractData


@pytest.fixture
def spark():
    return SparkSession.builder.appName('unit-tests').getOrCreate()

@pytest.fixture
def df_extract(spark):
    extract = ExtractData([2023], spark)
    df_extracted = extract.extract()
    return df_extracted

def test_count_df(df_extract):
    df_extracted = df_extract
    expected_schema = ['team', 'year', '# of Players', '90s Played',
                       'Yellow Cards', 'Red Cards', 'Second Yellow Card',
                       'Fouls Committed', 'Fouls Drawn', 'Offsides', 'Crosses',
                       'Interceptions', 'Tackles Won', 'Penalty Kicks Won',
                       'Penalty Kicks Conceded', 'Own Goals', 'Ball Recoveries',
                       'Aerials Won']



    # Assert schema
    assert df_extract.count() > 0

def test_schema(df_extract):
    df_extracted = df_extract

    schema_list = [
    ("team", StringType()),
    ("year", IntegerType()),
    ("# of Players", IntegerType()),
    ("90s Played", StringType()),
    ("Yellow Cards", IntegerType()),
    ("Red Cards", IntegerType()),
    ("Second Yellow Card", IntegerType()),
    ("Fouls Committed", IntegerType()),
    ("Fouls Drawn", IntegerType()),
    ("Offsides", IntegerType()),
    ("Crosses", IntegerType()),
    ("Interceptions", IntegerType()),
    ("Tackles Won", IntegerType()),
    ("Penalty Kicks Won", IntegerType()),
    ("Penalty Kicks Conceded", IntegerType()),
    ("Own Goals", IntegerType()),
    ("Ball Recoveries", IntegerType()),
    ("Aerials Won", IntegerType()),
]
    # Assert column data types (optional, adjust based on expected data)
    for col, dtype in enumerate(schema_list):
        print(dtype[1])
        
        assert df_extracted.schema[col].dataType == dtype[1]
