# Databricks notebook source
# MAGIC %md
# MAGIC ####Cette cellule crée une table Gold agrégée par devise. Elle transforme les transactions Silver en indicateur métier : l’encours total par devise, pays, score et émetteur . Cette table est adaptée au reporting et à la visualisation.
# MAGIC
# MAGIC ATTENTION : j'ai juste crée et affiché mais ce n'est pas sauvegardé

# COMMAND ----------

from pyspark.sql.functions import sum

df_gold_currency = (
    spark.read.table("silver_financial_trades")
    .groupBy("Currency")
    .agg(sum("Amount").alias("Total_Amount"))
)

df_gold_currency.show()

# COMMAND ----------

from pyspark.sql.functions import sum

df_gold_country = (
    spark.read.table("silver_financial_trades")
    .groupBy("Country")
    .agg(sum("Amount").alias("Total_Amount"))
)

df_gold_country.show()

# COMMAND ----------

from pyspark.sql.functions import sum

df_gold_rating = (
    spark.read.table("silver_financial_trades")
    .groupBy("Rating")
    .agg(sum("Amount").alias("Total_Amount"))
)

df_gold_rating.show()

# COMMAND ----------

from pyspark.sql.functions import sum

df_gold_issuer = (
    spark.read.table("silver_financial_trades")
    .groupBy("Issuer")
    .agg(sum("Amount").alias("Total_Amount"))
)

df_gold_issuer.show()

# COMMAND ----------

# MAGIC %md
# MAGIC on sauvegarde

# COMMAND ----------

df_gold_issuer.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_amount_by_issuer")


df_gold_country.write.format("delta").mode("overwrite").saveAsTable("gold_amount_by_country")
df_gold_rating.write.format("delta").mode("overwrite").saveAsTable("gold_amount_by_rating")

# COMMAND ----------

# MAGIC %md
# MAGIC on teste
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW Tables;

# COMMAND ----------

from pyspark.sql.functions import avg, datediff, current_date

df_gold_maturity = (
    spark.read.table("silver_financial_trades")
    .withColumn(
        "Days_To_Maturity",
        datediff("Maturity_Date", "Issue_Date")
    )
    .agg(
        avg("Days_To_Maturity")
        .alias("Average_Maturity_Days")
    )
)

df_gold_maturity.show()

# COMMAND ----------

df_gold_maturity.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("gold_average_maturity")

# COMMAND ----------

from pyspark.sql.functions import desc

df_top10_issuer = (
    spark.read.table("silver_financial_trades")
    .groupBy("Issuer")
    .agg(sum("Amount").alias("Total_Amount"))
    .orderBy(desc("Total_Amount"))
    .limit(10)
)