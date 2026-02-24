from pyspark.sql import Row
from pyspark.sql.functions import col, to_json, struct
from lib.Transformations import get_contract


def test_get_contract_creates_struct(spark):

    data = [
        Row(
            account_id="A1",
            account_start_date="2020-01-01",
            legal_title_1="Company1",
            legal_title_2="Owner1"
        )
    ]

    df = spark.createDataFrame(data)

    result_df = get_contract(df)

    # Check column created
    assert "contractIdentifier" in result_df.columns

    # Check struct value exists
    row = result_df.first()
    assert row.contractIdentifier.newValue == "A1"

    # Check row count
    assert result_df.count() == 1


def test_kafka_key_creation(spark):
    """
    Test Kafka key and value creation logic
    """

    data = [("A1",)]
    df = spark.createDataFrame(data, ["contractIdentifier"])

    df_kafka = df.select(
        col("contractIdentifier").alias("key"),
        to_json(struct("*")).alias("value")
    )

    row = df_kafka.first()

    assert row["key"] == "A1"
    assert isinstance(row["value"], str)