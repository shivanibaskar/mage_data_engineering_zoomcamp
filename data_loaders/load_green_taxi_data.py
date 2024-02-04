import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test



@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    Create a new pipeline, call it green_taxi_etl
    Add a data loader block and use Pandas to read data for the final quarter of 2020 (months 10, 11, 12).
    You can use the same datatypes and date parsing methods shown in the course.
    BONUS: load the final three months using a for loop and pd.concat

    """
    print("Getting data from the final quarter of 2020")
    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'trip_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'congestion_surcharge': float ,
        'ehail_fee' :float
    }
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    dataframes = []

    for month in ["2020-10","2020-11","2020-12"]:
        url =f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{month}.csv.gz"
        print(url)
        # Read the CSV file into a dataframe
        df = pd.read_csv(url, sep=',',compression = "gzip",dtype = taxi_dtypes,parse_dates=parse_dates)
        print(df.shape)
        # Append the dataframe to the list
        dataframes.append(df)

    # Concatenate the dataframes into a single dataframe
    df = pd.concat(dataframes)
    print("Once the dataset is loaded, what's the shape of the data?")
    print(df.shape)

    return df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
