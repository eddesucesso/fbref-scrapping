import requests
from bs4 import BeautifulSoup
from pyspark.sql import functions as f, DataFrame
from pyspark.sql.types import IntegerType




class ExtractData:
    def __init__(self, years: int, spark_session):
        """
        _summary_

        Args:
            years (int): years to webscrapping in website
            spark_session: SparkSession object
        """
        self.years = years
        self.spark_session = spark_session
        self.all_data = []  # Empty list to store final DataFrame

    def extract(self) -> DataFrame:
        """
        Functions that extract data from fbref and transforms to
        spark DataFrame directly

        Returns:
            DataFrame: Spark DataFrame that contains a lot of data from fbref
        """
        for year in self.years:
            print(year)
            url = f'https://fbref.com/en/comps/24/{year}/misc/{year}-Serie-A-Stats'  # noqa E501

            # Fetch the page
            response = requests.get(url)
            response.raise_for_status()  # Ensure the request was successful

            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')

            table = soup.find('table')
            columns = {th['data-stat']: th.get('aria-label', th.get('data-stat')) for th in table.find_all('th') if th.get('aria-label') or th.get('data-stat')}  # noqa E501
            columns['team_name'] = 'Team'

            # Process each row in the table for the current year
            year_data = []  # List to store data for the current year
            rows = table.find_all('tr')
            for row in rows:
                row_data = {}

                team_name_cell = row.find('th', {'class': 'left'})
                if team_name_cell and 'data-stat' in team_name_cell.attrs:
                    row_data[columns[team_name_cell['data-stat']]] = team_name_cell.text  # noqa E501

                for cell in row.find_all('td'):
                    data_stat = cell['data-stat']
                    if data_stat in columns:
                        row_data[columns[data_stat]] = cell.text
                if row_data:
                    row_data['year'] = year
                    year_data.append(row_data)

            # Append data for the current year to the final list
            self.all_data.extend(year_data)

        # Create DataFrame from the accumulated data
        data_df = self.spark_session.createDataFrame(self.all_data)

        casted_df = data_df.select(
                  f.col('team'),
                  f.col('year').cast(IntegerType()),
                  f.col('# of Players').cast(IntegerType()),
                  f.col('90s Played'),  # No cast needed for float
                  f.col('Yellow Cards').cast(IntegerType()),
                  f.col('Red Cards').cast(IntegerType()),
                  f.col('Second Yellow Card').cast(IntegerType()),
                  f.col('Fouls Committed').cast(IntegerType()),
                  f.col('Fouls Drawn').cast(IntegerType()),
                  f.col('Offsides').cast(IntegerType()),
                  f.col('Crosses').cast(IntegerType()),
                  f.col('Interceptions').cast(IntegerType()),
                  f.col('Tackles Won').cast(IntegerType()),
                  f.col('Penalty Kicks Won').cast(IntegerType()),
                  f.col('Penalty Kicks Conceded').cast(IntegerType()),
                  f.col('Own Goals').cast(IntegerType()),
                  f.col('Ball Recoveries').cast(IntegerType()),
                  f.col('Aerials Won').cast(IntegerType())
              )
        return casted_df
