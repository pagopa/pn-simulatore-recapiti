import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','mese_simulazione', 'id_simulazione_manuale', 'secretsManager_SecretId','jdbc_connection','s3_bucket'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


##################################
import boto3
import pyspark.sql.functions as F
import pyspark.sql.types as T
import ast
import json
import datetime


# recupero parametri d'ambiente del job
secretsManager_SecretId = args['secretsManager_SecretId']
jdbc_connection = args['jdbc_connection']
s3_bucket = args['s3_bucket']
mese_simulazione = args['mese_simulazione']
id_simulazione_manuale = args['id_simulazione_manuale']

print('Lettura dati input avviata')
######################################
# Import della tabella con i dati demografici di CAP e province

# recupero credenziali db da secretsmanager
client = boto3.client("secretsmanager")
response = client.get_secret_value(SecretId=secretsManager_SecretId)
response_SecretString = json.loads(response['SecretString'])

# CAP_PROV_REG
db_table = 'public."CAP_PROV_REG"'

df_cap_prov_reg = spark.read \
    .format("jdbc") \
    .option("url", jdbc_connection) \
    .option("dbtable", db_table) \
    .option("user", response_SecretString['username']) \
    .option("password", response_SecretString['password']) \
    .option("driver", "org.postgresql.Driver") \
    .load()


# CAPACITA_SIMULATE
db_table = 'public."CAPACITA_SIMULATE"'

df_capacita_simulate = spark.read \
    .format("jdbc") \
    .option("url", jdbc_connection) \
    .option("dbtable", db_table) \
    .option("user", response_SecretString['username']) \
    .option("password", response_SecretString['password']) \
    .option("driver", "org.postgresql.Driver") \
    .load()


# CAP_CAPACITIES

# Recupero path di lettura
target_date = datetime.date.today()
prefix = None
# inizializzazione connessione verso s3
s3_client = boto3.client('s3')
    
for _ in range(30):  # limite di sicurezza a 30 gg
    prefix = target_date.strftime("%Y/%m/%d/")
    response = s3_client.list_objects_v2(
        Bucket=s3_bucket,
        Prefix='input/'+prefix,
        MaxKeys=1
    )
    # se la cartella esiste, esco dal ciclo
    if 'Contents' in response:
        break
    # altrimenti vado al giorno precedente
    target_date -= datetime.timedelta(days=1)

# Lettura file
schema_cap_capacities = T.StructType() \
      .add("unifiedDeliveryDriver",T.StringType(),True) \
      .add("geoKey",T.StringType(),True) \
      .add("capacity",T.IntegerType(),True) \
      .add("peakCapacity",T.IntegerType(),True) \
      .add("activationDateFrom",T.TimestampType(),True) \
      .add("activationDateTo",T.TimestampType(),True) \
      .add("products",T.StringType(),True)

# Individuazione lista dei file csv con le capacità dei CAP nel path apposito
file_path = 's3://' + s3_bucket + '/'

file_list = s3_client.list_objects_v2(
        Bucket=s3_bucket,
        Prefix='input/' + prefix + mese_simulazione + '/cap_capacities/',
    )['Contents']
    
# Creazione di un unico dataframe con append dei dataframe corrispondenti ai file
df_cap_capacities = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema_cap_capacities)
for item in file_list:
    df_cap_capacities_part=spark.read.option("delimiter", ";").option("header",True).csv(file_path + item['Key'], schema=schema_cap_capacities)
    df_cap_capacities = df_cap_capacities.union(df_cap_capacities_part)

print('Lettura dati input completata')


# Aggiunta della colonna della provincia al dataframe CAP capacities

df_cap_capacities_prov = df_cap_capacities.join(df_cap_prov_reg, df_cap_capacities.geoKey == df_cap_prov_reg.CAP, how='left')\
                                          .select(df_cap_capacities['*'],df_cap_prov_reg['CodSiglaProvincia'])

print('Aggiunta della colonna provincia su CAP Capacities completata')


# Procedimento per l'aggiunta dei record su CAP Capacities in base al Flag default di Capacità simulate

def add_rows_cap(rows_type):
    
    df_capacita_simulate_check_flag = df_capacita_simulate.filter(F.col('FLAG_DEFAULT')==rows_type)
    
    schema_adding_rows = schema = T.StructType([
        T.StructField("unifiedDeliveryDriver", T.StringType(), True),
        T.StructField("geoKey", T.StringType(), True),
        T.StructField("capacity", T.IntegerType(), True),
        T.StructField("peakCapacity", T.IntegerType(), True),
        T.StructField("activationDateFrom", T.TimestampType(), True),
        T.StructField("activationDateTo", T.TimestampType(), True),
        T.StructField("products", T.StringType(), True),
        T.StructField("CodSiglaProvincia", T.StringType(), True),
        T.StructField("Chosen_capacity", T.IntegerType(), True)
    ])
    
    data_adding_rows = []
    
    for recap_prov in df_capacita_simulate_check_flag.select('UNIFIED_DELIVERY_DRIVER','COD_SIGLA_PROVINCIA').distinct().collect():
    
        # Individuazione dei record per la coppia provincia-recapitista nella CAP capacities e nella capacità simulate
        df_cap_capacities_prov_filtred = df_cap_capacities_prov.filter((F.col('unifiedDeliveryDriver')==recap_prov['UNIFIED_DELIVERY_DRIVER']) & 
                                                                       (F.col('CodSiglaProvincia')==recap_prov['COD_SIGLA_PROVINCIA'])).collect()

        df_capacita_simulate_recap_prov = df_capacita_simulate_check_flag.filter((F.col('UNIFIED_DELIVERY_DRIVER')==recap_prov['UNIFIED_DELIVERY_DRIVER']) & 
                                                                         (F.col('COD_SIGLA_PROVINCIA')==recap_prov['COD_SIGLA_PROVINCIA'])).collect()
        
        # Aggiunta di un record con la corretta capacità per ogni CAP-settimana presente in CAP capacities, in base ad ogni settimana della capacità simulate
        for row_cap in df_cap_capacities_prov_filtred:
            for row_capsim in df_capacita_simulate_recap_prov:
                data_adding_row = {'unifiedDeliveryDriver': recap_prov['UNIFIED_DELIVERY_DRIVER'],
                                     'geoKey': row_cap['geoKey'],
                                     'capacity': row_cap['capacity'],
                                     'peakCapacity': row_cap['peakCapacity'],
                                     'activationDateFrom': row_capsim['ACTIVATION_DATE_FROM'],
                                     'activationDateTo': row_capsim['ACTIVATION_DATE_TO'],
                                     'products': row_cap['products'],
                                     'CodSiglaProvincia': recap_prov['COD_SIGLA_PROVINCIA'],
                                     # Se rows_type è True aggiungo la capacità di BAU, altrimenti quella di picco
                                     'Chosen_capacity': row_cap['capacity']*(rows_type==True) + row_cap['peakCapacity']*(rows_type==False)}
                
                data_adding_rows.append(data_adding_row)
    
    
    adding_rows = spark.createDataFrame(data_adding_rows, schema=schema_adding_rows)
    adding_rows_stg = adding_rows.withColumn('capacity',F.col('Chosen_capacity'))\
                                 .withColumn('peakCapacity',F.col('Chosen_capacity'))
    
    df_cap_capacities_final = df_cap_capacities_prov.union(adding_rows_stg)

    return df_cap_capacities_final


# Stabilire se tutti i record di capacità simulate con active date to NULL hanno anche flag default NULL
count_datetonull = df_capacita_simulate.filter(F.col('ACTIVATION_DATE_TO').isNull()).count()
count_flag_F = df_capacita_simulate.filter((F.col('ACTIVATION_DATE_TO').isNull()) & (F.col('FLAG_DEFAULT')==False)).count()

# Se i flag sono tutti false aggiungere righe con capacità di picco, altrimenti con capacità di BAU
if count_datetonull == count_flag_F:
    
    df_cap_capacities_prov = df_cap_capacities_prov.withColumn("Chosen_capacity",F.col('peakCapacity'))
    df_cap_capacities_final = add_rows_cap(rows_type=True)
    # In questo caso vengono anche modificati i record esistenti, inserendo la capacità di picco al posto di quella di BAU
    df_cap_capacities_final = df_cap_capacities_final.withColumn("capacity",F.col('Chosen_capacity'))
    
else:
    
    df_cap_capacities_prov = df_cap_capacities_prov.withColumn("Chosen_capacity",F.col('capacity'))
    df_cap_capacities_final = add_rows_cap(rows_type=False)

df_cap_capacities_final = df_cap_capacities_final.withColumnRenamed('unifiedDeliveryDriver','UNIFIED_DELIVERY_DRIVER')\
                                                 .withColumnRenamed('geoKey','GEOKEY')\
                                                 .withColumnRenamed('capacity','CAPACITY')\
                                                 .withColumnRenamed('peakCapacity','PEAK_CAPACITY')\
                                                 .withColumnRenamed('activationDateFrom','ACTIVATION_DATE_FROM')\
                                                 .withColumnRenamed('activationDateTo','ACTIVATION_DATE_TO')\
                                                 .withColumn('LAST_UPDATE_TIMESTAMP',F.current_timestamp())\
                                                 .withColumn('SIMULAZIONE_ID',F.lit(id_simulazione_manuale))\
                                                 .select('UNIFIED_DELIVERY_DRIVER','ACTIVATION_DATE_FROM','ACTIVATION_DATE_TO','CAPACITY','PEAK_CAPACITY','GEOKEY','LAST_UPDATE_TIMESTAMP','SIMULAZIONE_ID')



print('Export su DB')      
# Export su DB
 
db_table='public."CAPACITA_SIMULATE_CAP"'
 
df_cap_capacities_final.write \
    .format("jdbc") \
    .option("url", jdbc_connection) \
    .option("dbtable", db_table) \
    .option("user", response_SecretString['username']) \
    .option("password", response_SecretString['password']) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()


# da lasciare come ultimo comando per indicare che il job ha terminato con SUCCESS la sua esecuzione
job.commit()