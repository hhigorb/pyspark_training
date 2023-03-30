from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('app_name') \
    .getOrCreate()

schema = 'nome STRING, postagem STRING, data INT'

df = spark.readStream.json(path='dados_de_exemplo/streaming_teste/',
                           schema=schema)

diretorio = 'temp'

stcal = df.writeStream.format('console') \
        .outputMode('append') \
        .trigger(processingTime='5 second') \
        .option('checkpointlocation', diretorio) \
        .start()
        
stcal.awaitTermination()
