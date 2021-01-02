// https://towardsdatascience.com/six-spark-exercises-to-rule-them-all-242445b24565

// Exercise 1

// Get top 100 skewed keys
skewedRows = sales_table
    .groupby("product_id")
    .count()
    .sort(col("count").desc)
    .limit(100)
    .collect()

REPLICATION_FACTOR = 100
var replicated_products = scala.collection.mutable.Set[String]
var l = List()
for (row <- skewedRows) {
    // TODO - investigate NULL handling when product_id col has null values
    replicated_products += row.getAs[String]("product_id")
}

// Salt the product table
products_table = products_table
    .join(broadcast(replicated_df), "product_id", "left")
    .withColumn(
        "salted_key", 
        when(replicated_df["replication"].isNull, products_table["product_id"])
            .otherwise(concat(
                replicated_df["product_id"], 
                lit("-"), 
                replicated_df["replication"]))
    )

// Salt the sale table
sales_table = sales_table
    .withColumn(
        "salted_key",
        when(sales_table("product_id").isin(replicated_products))
            .otherwise(sales_table("product_id"))
    )

// Join product and sale on salted key
sales_table
    .join(products_table, "salted_key", "inner")
    .agg(avg(products_table["price"] * sales_table["num_pieces_sold"]))
    .show()
