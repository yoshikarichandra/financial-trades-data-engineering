# Databricks notebook source
# MAGIC %md
# MAGIC ####Cette cellule lit le fichier CSV brut stocké dans un Volume Unity Catalog et le charge dans un DataFrame Spark. L’option header=true indique que la première ligne contient les noms de colonnes. L’option inferSchema=true permet à Spark de détecter automatiquement les types de données. Le show(5) affiche les 5 premières lignes pour vérifier que l’ingestion fonctionne

# COMMAND ----------

file_path = "/Volumes/workspace/default/bronze_files/transactions_75k.csv"

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(file_path)
)

df_raw.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cette cellule sauvegarde les données brutes dans une table Delta Bronze. La couche Bronze conserve les données proches de la source, sans nettoyage métier. Le format Delta permet de bénéficier du stockage fiable, de l’historique et des transactions ACID.

# COMMAND ----------

df_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_financial_trades")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cette requête vérifie que les 75 000 lignes du fichier source ont bien été chargées dans la table Bronze.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM bronze_financial_trades;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cette cellule affiche la structure des colonnes et leurs types. Elle permet de vérifier que Spark a correctement reconnu les champs comme Amount en double, les dates en date, et les autres colonnes en string.

# COMMAND ----------

df_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cette cellule identifie les différentes valeurs présentes dans la colonne Currency. Elle permet de détecter les incohérences comme EURO, Eur, eur, Gbp, GBP., USDD ou les valeurs nulles.

# COMMAND ----------

df_raw.groupBy("Currency") \
      .count() \
      .orderBy("count", ascending=False) \
      .show(20, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cette cellule compte les transactions avec un montant négatif. Ces valeurs sont considérées comme des anomalies métier dans ce projet et seront corrigées dans la couche Silver.

# COMMAND ----------

from pyspark.sql.functions import col

df_raw.filter(col("Amount") < 0).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cette cellule compte le nombre de Trade_ID apparaissant plusieurs fois. Elle permet d’identifier les doublons à supprimer lors du passage en couche Silver

# COMMAND ----------

df_raw.groupBy("Trade_ID") \
      .count() \
      .filter("count > 1") \
      .count()

# COMMAND ----------

# MAGIC %md
# MAGIC a supprimer ce quil y a en bas
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cette cellule transforme les données Bronze en données Silver. Elle standardise les devises, corrige les montants négatifs avec abs(), supprime les doublons sur Trade_ID, puis sépare les lignes valides des lignes en erreur. Les lignes sans devise sont isolées car la devise est indispensable pour les analyses financières.

# COMMAND ----------

from pyspark.sql.functions import col, upper, trim, when, abs

df_bronze = spark.read.table("bronze_financial_trades")

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

# COMMAND ----------

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
# MAGIC #### On sauvegarde 

# COMMAND ----------

df_gold_issuer.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_amount_by_issuer")


df_gold_country.write.format("delta").mode("overwrite").saveAsTable("gold_amount_by_country")
df_gold_rating.write.format("delta").mode("overwrite").saveAsTable("gold_amount_by_rating")

# COMMAND ----------

# MAGIC %md
# MAGIC ####On teste

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW Tables;