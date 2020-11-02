from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.streaming import StreamingContext

spark = SparkSession.builder.appName("AppName").getOrCreate()
spark.sparkContext.setLogLevel('WARN') #Quitar mensajes de INFO

#leer el data set conitniuo desde el csv, cambiar por streaming
"""df = spark.read.load('adult_set.csv',
                  format='com.databricks.spark.csv',
                  header='true',
                  inferSchema='true').cache()"""
cSchema = StructType([StructField("x", StringType()), StructField("age", StringType()), StructField("workclass", StringType()), StructField("fnlwgt", StringType()), StructField("education", StringType()), StructField("educational-num", StringType()), StructField("marital-status", StringType()), StructField("occupation", StringType()), StructField("relationship", StringType()), StructField("race", StringType()), StructField("gender", StringType()), StructField("capital-gain", StringType()), StructField("capital-loss", StringType()), StructField("hours-per-week", StringType()), StructField("native-country", StringType()), StructField("income", StringType())])
l = [['1', '25', 'Private', '226802', '11th', '7', 'Never-married', 'Machine-op-inspct', 'Own-child', 'Black', 'Male', '0', '0', '40', 'United-States', '<=50K'], ['2', '38', 'Private', '89814', 'HS-grad', '9', 'Married-civ-spouse', 'Farming-fishing', 'Husband', 'White', 'Male', '0', '0', '50', 'United-States', '<=50K']]
df = spark.createDataFrame(l,schema=cSchema)
df.show()
df = df.drop("x")
df.show()
"""
#valores originales
#print("Filas {0} Columnas {1}".format(df.count(), len(df.columns)))

#data cleaning
def replace(column, value):
    return when(column != value, column).otherwise(lit(None))

df = df.withColumn("occupation", replace(col("occupation"), "?"))
df = df.withColumn("native_country", replace(col("native_country"), "?"))
df = df.na.drop()

for c in df.columns:
  if(isinstance(df.schema[c].dataType, StringType)):
    indexer = StringIndexer(inputCol= c, outputCol=c.capitalize())
    df = indexer.fit(df).transform(df)

#valores despues del data cleaning
#print("Filas {0} Columnas {1}".format(df.count(), len(df.columns)))

#Machine learning
assemblerAtributos= VectorAssembler(inputCols=["age","Workclass","fnlwgt", "Education", "educational_num", "Marital_status", "Occupation", "Relationship", "Race", "Gender", "capital_gain", "capital_loss","hours_per_week", "Native_country"], outputCol= "Atributos")
dfModificado = assemblerAtributos.transform(df)
dfModificado= dfModificado.select("Atributos","Income")
train, test = dfModificado.randomSplit([0.8,0.2],seed=1) #80% entrenamiento 20% test
#dfModificado.show()

#Aplicamos la tecnica de GBT
GPT = GBTClassifier(featuresCol="Atributos", labelCol="Income", maxBins=41)
GPT = GPT.fit(train)
predictions = GPT.transform(test)
cm = predictions.select("Income", "prediction")
cm.groupby('Income').agg({'Income': 'count'}).show()
cm.groupby('prediction').agg({'prediction': 'count'}).show()

print(cm.filter(cm.Income == cm.prediction).count() / cm.count())
"""
spark.stop()
