import os


# -----------------------------
# READ ACCOUNTS
# -----------------------------
def read_accounts(spark, environment, base_path):

    if environment == "LOCAL":

        file_path = os.path.join(base_path, "account_samples.csv")

        df = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(file_path)

        return df

    else:
        # Hive logic will come later
        pass


# -----------------------------
# READ PARTIES
# -----------------------------
def read_parties(spark, environment, base_path):

    if environment == "LOCAL":

        file_path = os.path.join(base_path, "party_samples.csv")

        df = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(file_path)

        return df

    else:
        # Hive logic will come later
        pass


# -----------------------------
# READ ADDRESS
# -----------------------------
def read_address(spark, environment, base_path):

    if environment == "LOCAL":

        file_path = os.path.join(base_path, "address_samples.csv")

        df = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(file_path)

        return df

    else:
        # Hive logic will come later
        pass
