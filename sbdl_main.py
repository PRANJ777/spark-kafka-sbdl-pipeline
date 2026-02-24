import sys
import os
from lib.Utils import get_spark_session
from lib.ConfigLoader import get_config
from lib.DataLoader import read_accounts, read_parties, read_address
from lib.Transformations import get_contract, get_party , get_address
from pyspark.sql.functions import struct
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import when, array, col
from pyspark.sql.functions import to_json
from lib.loggers import get_logger


def main():

    if len(sys.argv) != 3:
        print("Usage: python sbdl_main.py <ENV> <LOAD_DATE>")
        sys.exit(1)

    environment = sys.argv[1]
    load_date = sys.argv[2]

    base_path = os.path.join("tests", "data")

    # -------------------------
    # Create Spark Session
    # -------------------------
    spark = get_spark_session(environment)

    logger = get_logger("SBDL_MAIN")

    # -------------------------
    # Load Config
    # -------------------------
    config = get_config(environment)

    kafka_topic = config["kafka.topic"]
    bootstrap_servers = config["kafka.bootstrap.servers"]

    print("Kafka Topic:", kafka_topic)
    print("Kafka Bootstrap Servers:", bootstrap_servers)

    # -------------------------
    # Read Accounts
    # -------------------------
    accounts_df = read_accounts(spark, environment, base_path)
    logger.info(f"Accounts Count: {accounts_df.count()}")

    # -------------------------
    # Transform Accounts
    # -------------------------
    contract_df = get_contract(accounts_df)

    print("Transformed Accounts Schema:")
    contract_df.printSchema()

    print("Sample Transformed Data:")
    contract_df.show(5, truncate=False)

    # -------------------------
    # Read Parties
    # -------------------------
    parties_df = read_parties(spark, environment, base_path)
    print("Parties Count:", parties_df.count())

    # -------------------------
# Transform Parties
# -------------------------
    party_transformed_df = get_party(parties_df)

    print("Transformed Parties Schema:")
    party_transformed_df.printSchema()

    print("Sample Transformed Parties:")
    party_transformed_df.show(5, truncate=False)

    # -------------------------
    # Read Address
    # -------------------------
    address_df = read_address(spark, environment, base_path)
    print("Address Count:", address_df.count())

    address_transformed_df = get_address(address_df)

    print("Transformed Address Schema:")
    address_transformed_df.printSchema()

    print("Sample Transformed Address:")
    address_transformed_df.show(5, truncate=False)

    # -------------------------
# Join Party + Address
# -------------------------
    party_address_joined_df = party_transformed_df.join(
    address_transformed_df,
    on="party_id",
    how="left"
)

    print("Joined Party + Address Schema:")
    party_address_joined_df.printSchema()

    print("Sample Joined Data:")
    party_address_joined_df.show(5, truncate=False)

    # -------------------------
# Create partyAddress struct
# -------------------------
    party_with_address_df = party_address_joined_df.withColumn(
    "partyAddress",
    struct(
        "addressLine1",
        "addressLine2",
        "city",
        "state",
        "country"
    )
)

    print("Party with Address Struct:")
    party_with_address_df.printSchema()
    party_with_address_df.show(5, truncate=False)

    # -------------------------
# Create Full Party Struct
# -------------------------

    full_party_df = party_with_address_df.withColumn(
    "party",
    struct(
        "partyIdentifier",
        "partyRelationshipType",
        "partyRelationStartDateTime",
        "partyAddress"
    )
)

    print("Full Party Struct Schema:")
    full_party_df.printSchema()

    print("Sample Full Party:")
    full_party_df.select("account_id", "party").show(5, truncate=False)

    #GROUPBY

    grouped_party_df = full_party_df.groupBy("account_id") \
    .agg(collect_list("party").alias("party"))

    print("Grouped Party Schema:")
    grouped_party_df.printSchema()

    print("Sample Grouped Party:")
    grouped_party_df.show(5, truncate=False)

    final_df = contract_df.join(
    grouped_party_df,
    contract_df.contractIdentifier.newValue == grouped_party_df.account_id,
    "left"
)
    
    final_df = final_df.withColumn(
    "party",
    when(col("party").isNull(), array()).otherwise(col("party"))
)

    

    final_df = final_df.drop(grouped_party_df.account_id)

    kafka_df = final_df.select(
    struct(
        col("contractIdentifier"),
        col("contractStartDateTime"),
        col("contractTitle"),
        col("party")
    ).alias("value")
)
    
    kafka_df = kafka_df.select(
    to_json(col("value")).alias("value")
)
    try:
         kafka_df.show(5, truncate=False)

         kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("topic", kafka_topic) \
        .save()

         logger.info("Data successfully written to Kafka")

    except Exception as e:
        logger.error("Failed while writing to Kafka", exc_info=True)
        raise e
    
    print(f"Spark started in {environment} mode")
    print(f"Processing load date: {load_date}")

    spark.stop()
    


if __name__ == "__main__":
    main()
