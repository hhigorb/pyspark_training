from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('app_name') \
    .config("spark.driver.extraClassPath", "jdbc_drivers/postgresql-42.2.24.jar") \
    .getOrCreate()

schema = 'nome STRING, postagem STRING, data INT'

df = spark.readStream.json(path='dados_de_exemplo/streaming_teste/',
                           schema=schema)

diretorio = 'temp'


def update_postgres(df, batch_id):
    df.write.format('jdbc') \
        .option('url', 'jdbc:postgresql://postgres-database.chcgqj6jrjf8.us-west-2.rds.amazonaws.com/posts') \
        .option('dbtable', 'posts') \
        .option('user', 'postgres') \
        .option('password', '123456789') \
        .option('driver', 'org.postgresql.Driver') \
        .mode('append') \
        .save()
   
stcal = df.writeStream.foreachBatch(update_postgres) \
        .outputMode('append') \
        .trigger(processingTime='5 second') \
        .option('checkpointLocation', diretorio) \
        .start()
    
stcal.awaitTermination()
