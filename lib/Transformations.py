from pyspark.sql.functions import lit, struct, col, array


# -----------------------------------
# CREATE INSERT OPERATION TRIPLET
# -----------------------------------
def get_insert_operation(column):

    return struct(
        lit("INSERT").alias("operation"),
        column.alias("newValue"),
        lit(None).alias("oldValue")
    )


# -----------------------------------
# TRANSFORM ACCOUNT TABLE
# -----------------------------------
def get_contract(accounts_df):

    contract_df = accounts_df.select(
        get_insert_operation(col("account_id")).alias("contractIdentifier"),
        get_insert_operation(col("account_start_date")).alias("contractStartDateTime"),
        array(
            get_insert_operation(col("legal_title_1")),
            get_insert_operation(col("legal_title_2"))
        ).alias("contractTitle")
    )

    return contract_df


# -----------------------------------
# TRANSFORM PARTY TABLE
# -----------------------------------
def get_party(parties_df):

    party_df = parties_df.select(
        col("account_id"),
        col("party_id"), 
        get_insert_operation(col("party_id")).alias("partyIdentifier"),
        get_insert_operation(col("party_relation_type")).alias("partyRelationshipType"),
        get_insert_operation(col("load_date")).alias("partyRelationStartDateTime")
    )

    return party_df

# -----------------------------------
# TRANSFORM ADDRESS TABLE
# -----------------------------------
def get_address(address_df):

    address_transformed_df = address_df.select(
        col("party_id"),

        get_insert_operation(col("address_line_1")).alias("addressLine1"),

        get_insert_operation(col("address_line_2")).alias("addressLine2"),

        get_insert_operation(col("city")).alias("city"),

        get_insert_operation(col("state")).alias("state"),

        get_insert_operation(col("country")).alias("country")
    )

    return address_transformed_df