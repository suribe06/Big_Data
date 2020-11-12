from confluent_kafka import Consumer
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.streaming import StreamingContext
from pyspark.mllib.evaluation import MulticlassMetrics

#Iniciar spark
spark = SparkSession.builder.appName("AppName").getOrCreate()
spark.sparkContext.setLogLevel('WARN') #Quitar mensajes de INFO

def consumer_act():
    c = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'testing',
        'auto.offset.reset': 'earliest'
    })
    my_topic = "test"
    c.subscribe([my_topic])

    number_of_rows, cont, batch_size = 48842, 0, 7000
    batch_num = 1
    batch = []
    preBatch = []
    tb = None
    while cont < number_of_rows:
        msg = c.poll(1.0)
        if msg is None:
            continue
        elif msg.error():
            print("Consumer error: {0}".format(msg.error()))
            continue
        else:
            #Valor del mensaje obtenido
            msg_val = msg.value().decode('utf-8').strip().split(",")
            #Parseo del menaje
            msg_val[0], msg_val[1], msg_val[3], msg_val[5], msg_val[11], msg_val[12], msg_val[13] = int(msg_val[0]), int(msg_val[1]), int(msg_val[3]), int(msg_val[5]), int(msg_val[11]), int(msg_val[12]), int(msg_val[13])
            #se agrega el mensaje al batch
            batch.append(msg_val)
            remaining = number_of_rows - (cont + 1)
            if len(batch) == batch_size or (len(batch) == number_of_rows%batch_size and remaining < batch_size):
                #Se envia el batch a procesar
                G = batch_processing(batch)
                if batch_num <= 6:preBatch.append(G)
                else: tb = G #el ultimo batch sera el usado para testear los modelos de los demas batches
                batch_num += 1
                batch = []
        cont += 1
    #Se calcula las predicciones para los batches 1 a 6
    for b in preBatch:
        predictions(b,tb)
    c.close()

def replace(column, value):
    return when(column != value, column).otherwise(lit(None))

def batch_processing(batch):
    global spark
    #Esquema del data frame
    cSchema = StructType([StructField("x", IntegerType()), StructField("age", IntegerType()), StructField("workclass", StringType()), StructField("fnlwgt", IntegerType()), StructField("education", StringType()), StructField("educational_num", IntegerType()), StructField("marital_status", StringType()), StructField("occupation", StringType()), StructField("relationship", StringType()), StructField("race", StringType()), StructField("gender", StringType()), StructField("capital_gain", IntegerType()), StructField("capital_loss", IntegerType()), StructField("hours_per_week", IntegerType()), StructField("native_country", StringType()), StructField("income", StringType())])
    df = spark.createDataFrame(batch, schema=cSchema)

    #Data cleaning
    df = df.drop("x")
    df = df.withColumn("occupation", replace(col("occupation"), "?"))
    df = df.withColumn("native_country", replace(col("native_country"), "?"))
    df = df.na.drop()
    for c in df.columns:
      if(isinstance(df.schema[c].dataType, StringType)):
        indexer = StringIndexer(inputCol= c, outputCol=c.capitalize())
        df = indexer.fit(df).transform(df)

    assemblerAtributos= VectorAssembler(inputCols=["age", "Workclass", "fnlwgt", "Education", "educational_num", "Marital_status", "Occupation", "Relationship", "Race", "Gender", "capital_gain", "capital_loss", "hours_per_week", "Native_country"], outputCol= "Atributos")
    dfModificado = assemblerAtributos.transform(df)
    dfModificado = dfModificado.select("Atributos","Income")
    return dfModificado

def predictions(train,test):
    #Aplicamos la tecnica de GBT
    GPT = GBTClassifier(featuresCol="Atributos", labelCol="Income", maxBins=41)
    GPT = GPT.fit(train)
    predictions = GPT.transform(test)
    results = predictions.select("Income", "prediction")
    predictionAndLabels = results.rdd
    metrics = MulticlassMetrics(predictionAndLabels)
    cm = metrics.confusionMatrix().toArray()
    #Calculo de metricas
    accuracy = (cm[0][0] + cm[1][1]) / cm.sum()
    precision = cm[0][0] / (cm[0][0] + cm[1][0])
    recall = cm[0][0] / (cm[0][0] + cm[0][1])
    f1 = 2 * ((precision * recall) / (precision + recall))
    print("Metricas del modelo GBT Classifier")
    print("accuracy = {0}, precision = {1}, recall = {2}, f1 = {3}".format(accuracy, precision, recall, f1))
    return

consumer_act()
spark.stop()
