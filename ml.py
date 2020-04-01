from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, NaiveBayes, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, StringIndexer
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col

NUMERO_MAXIMO_CPUS = 3
SPARK_MASTER = "local[" + str(NUMERO_MAXIMO_CPUS) + "]"

session = SparkSession \
    .builder \
    .master(SPARK_MASTER) \
    .appName("SQL") \
    .getOrCreate()


df = session.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('train.csv')
# df.show()
# df.printSchema()
#
#
# df.groupby('DayOfWeek').count().show()
#
# #print(df.groupby('Category').count().orderBy('count', ascending=False).take(10))
#
# df.groupby('Category').count().orderBy('count', ascending=False).limit(10).show()
#
# for columna in df.columns:
#     print(columna, '  =  ', df.filter(df[columna].isNull()).count())
# # Media X, Y para Crimenes : Wednesday x CATEGORIAS
# df.describe('X').show()
# session.stop()

#df.filter(df['DayOfWeek']=='Wednesday').groupBy('Category').avg().show()

df.show()

datos_modelo=df.select(['Category','Descript'])
datos_modelo.show()

print(datos_modelo.select('Category').distinct().count())
print(datos_modelo.select('Descript').distinct().count())
datos_modelo.select(['Category','Descript']).distinct().groupBy('Category').count().show()


palabras_vacias=['from']

tokenizador=RegexTokenizer(inputCol='Descript', outputCol='palabras', pattern='\\W')
filtrador=StopWordsRemover(inputCol='palabras', outputCol='palabras_filtradas').setStopWords(palabras_vacias)
contador=CountVectorizer(inputCol='palabras_filtradas', outputCol='features', vocabSize=20000, minDF=10)
indexador=StringIndexer(inputCol='Category', outputCol='label')

preparacion=Pipeline(stages=[tokenizador,filtrador,contador,indexador])
preparacion_ajustada=preparacion.fit(datos_modelo)
datos_preparados=preparacion_ajustada.transform(datos_modelo)


datos_preparados.show()
(entrenamiento,prueba) = datos_preparados.randomSplit([0.75 , 0.25], seed= 654)

#algoritmo=LogisticRegression(maxIter=30)
#algoritmo=NaiveBayes()
algoritmo=RandomForestClassifier(maxBins=40)
modelo = algoritmo.fit(entrenamiento)
predicciones=modelo.transform(prueba)
predicciones.show()

predicciones.filter(col('label')!=col('prediction')).show()

evaluador=MulticlassClassificationEvaluator(predictionCol='prediction')
print(evaluador.evaluate(predicciones))



df = session.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('test.csv')
datos_preparados=preparacion_ajustada.transform(df)
predicciones=modelo.transform(prueba)
