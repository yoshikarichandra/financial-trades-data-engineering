# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ####Cette cellule transforme les données Bronze en données Silver. Elle standardise les devises, corrige les montants négatifs avec abs(), supprime les doublons sur Trade_ID, puis sépare les lignes valides des lignes en erreur. Les lignes sans devise sont isolées car la devise est indispensable pour les analyses financières.

# COMMAND ----------

from pyspark.sql.functions import col, upper, trim, when, abs

df_bronze = spark.read.table("bronze_financial_trades")

# COMMAND ----------

df_silver = (
    df_bronze
    .withColumn("Currency", upper(trim(col("Currency"))))
    .withColumn(
        "Currency",
        when(col("Currency").isin("EURO", "EUR"), "EUR")
        .when(col("Currency").isin("USDD", "USD"), "USD")
        .when(col("Currency").isin("GBP.", "GBP"), "GBP")
        .otherwise(col("Currency"))
    )
    .withColumn("Amount", abs(col("Amount")))
    .dropDuplicates(["Trade_ID"])
)

# COMMAND ----------

df_silver_valid = df_silver.filter(col("Currency").isNotNull())

df_silver_errors = df_silver.filter(col("Currency").isNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cette cellule sauvegarde la table Silver nettoyée et une table d’erreurs. La table Silver contient les transactions fiables pour les analyses. La table d’erreurs conserve les lignes rejetées afin de garder une traçabilité des anomalies.

# COMMAND ----------

df_silver_valid.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_financial_trades")

# COMMAND ----------

df_silver_errors.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_financial_trades_errors")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cette cellule vérifie que les devises ont bien été standardisées. Après nettoyage, seules les devises valides doivent apparaître : EUR, USD, GBP, JPY, CNY, CHF, AUD, CAD.

# COMMAND ----------

spark.read.table("silver_financial_trades").groupBy("Currency").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cette cellule vérifie qu’il ne reste plus aucun montant négatif dans la table Silver.

# COMMAND ----------

from pyspark.sql.functions import col

spark.read.table("silver_financial_trades") \
    .filter(col("Amount") < 0) \
    .count()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cette cellule compte le nombre de lignes isolées dans la table d’erreurs, notamment les transactions dont la devise est manquante.

# COMMAND ----------

spark.read.table("silver_financial_trades_errors").count()