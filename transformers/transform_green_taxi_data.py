if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Add a transformer block and perform the following:
    Remove rows where the passenger count is equal to 0 or the trip distance is equal to zero.
    Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id.
    Add three assertions:
    vendor_id is one of the existing values in the column (currently)
    passenger_count is greater than 0
    trip_distance is greater than 0

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    data = data[data['passenger_count']>0]
    data = data[data['trip_distance']>0]

    print("Upon filtering the dataset where the passenger count is greater than 0 and the trip distance is greater than zero, how many rows are left")
    print(data.shape)

    print("What are the existing values of VendorID in the dataset?")
    print(set(data["VendorID"]))

    data["lpep_pickup_date"] = data["lpep_pickup_datetime"].dt.date
    print(len(set(data["lpep_pickup_date"])))
    data.columns = (data.columns
                .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                .str.lower()
             )
    
    print("What are the existing values of VendorID in the dataset after filtering?")
    print(set(data["vendor_id"]))
    

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output[output['passenger_count']<=0]['passenger_count'].count() == 0, 'There are rides with <=0  passengers'
    assert output[output['trip_distance']<=0]['trip_distance'].count() == 0, 'There are rides with a <=0 trip distance'
    assert "vendor_id" in output.columns,"There is no vendor_id column"
    assert output is not None, 'The output is undefined'
