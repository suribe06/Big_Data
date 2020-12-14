#Librerias para pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.ml.evaluation import RegressionEvaluator

#Conexion a spark
spark = SparkSession.builder.appName("AppName").getOrCreate()
spark.sparkContext.setLogLevel('WARN') #Quitar mensajes de INFO
spark = SparkSession.builder.appName("AppName").getOrCreate()
#Creacion del esquema del data frame
cSchema = StructType([StructField("Page ID", IntegerType()),StructField("Name", StringType()), StructField("Creation Date",TimestampType()),StructField("type", StringType()), StructField("Louvain Community", IntegerType()),StructField("Final In Degree", IntegerType()),StructField("EC Estimate",FloatType()), StructField("Fraction of total pageviews (July 2015)",FloatType()), StructField("Number of Edits", IntegerType()), StructField("Unique Editors", IntegerType()), StructField("Number of Talk Page Edits", IntegerType()),StructField("Unique Talk Page Editors",IntegerType()),StructField("Page Size",IntegerType())])
#Cargar datos
df = spark.read.option("delimiter", "\t").option("header", "true").schema(cSchema).csv('nodes.csv') #datos originales
df2 = spark.read.load('graph_metrics.csv', format='com.databricks.spark.csv', header='true', inferSchema='true').cache() #metricas de grafos
#Creacion del nuevo data frame (original + metrica de grafos)
new_schema = StructType(df.schema.fields + df2.schema.fields)
df1df2 = df.rdd.zip(df2.rdd).map(lambda x: x[0]+x[1])
new_df = spark.createDataFrame(df1df2, new_schema)

# Data cleaning
new_df = new_df.drop("Page ID")
new_df = new_df.drop("Final In Degree")
new_df = new_df.drop("Name")
new_df = new_df.drop("Creation Date")

for c in new_df.columns:
    if(isinstance(new_df.schema[c].dataType, StringType)):
        indexer = StringIndexer(inputCol= c, outputCol=c.capitalize())
        new_df = indexer.fit(new_df).transform(new_df)

#Machine Learning (Linear Regression)
assemblerAtributos = VectorAssembler(inputCols=["Type","Louvain Community", "EC Estimate", "Fraction of total pageviews (July 2015)", "Unique Editors", "Number of Talk Page Edits", "Unique Talk Page Editors", "Page Size", "Indegree", "Outdegree", "NumSCC", "betweenness", "closeness"], outputCol= "Atributos")
dfModificado = assemblerAtributos.transform(new_df)
dfModificado = dfModificado.select("Atributos","Number of Edits")
train, test = dfModificado.randomSplit([0.7,0.3],seed=1)
lr = LinearRegression(featuresCol = 'Atributos', labelCol='Number of Edits', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train)
trainingSummary = lr_model.summary
lr_predictions = lr_model.transform(test)
#metricas para regresion
test_result = lr_model.evaluate(test)
print("Root Mean Squared Error (RMSE) = {0}".format(test_result.rootMeanSquaredError))
print("R Squared (R^2) = {0}".format(test_result.r2))
print("Mean Absolute Error (MAE) = {0}".format(test_result.meanAbsoluteError))
spark.stop()
