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
id_simulazione_manuale = int(args['id_simulazione_manuale'])

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
        Prefix='input/' + prefix + mese_simulazione[:7] + '/cap_capacities/',
    )['Contents']
    
# Creazione di un unico dataframe con append dei dataframe corrispondenti ai file
df_cap_capacities = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema_cap_capacities)
for item in file_list:
    df_cap_capacities_part=spark.read.option("delimiter", ";").option("header",True).csv(file_path + item['Key'], schema=schema_cap_capacities)
    df_cap_capacities = df_cap_capacities.union(df_cap_capacities_part)

print('Lettura dati input completata')


# Aggiunta della colonna della provincia al dataframe CAP capacities

df_cap_capacities_prov = df_cap_capacities.join(df_cap_prov_reg, df_cap_capacities.geoKey == df_cap_prov_reg.CAP, how='left')\
                                          .select(df_cap_capacities['*'],df_cap_prov_reg['COD_SIGLA_PROVINCIA'])

print('Aggiunta della colonna provincia su CAP Capacities completata')


# Procedimento per l'aggiunta dei record su CAP Capacities in base al Flag default di Capacità simulate

schema_adding_rows = schema = T.StructType([
        T.StructField("unifiedDeliveryDriver", T.StringType(), True),
        T.StructField("geoKey", T.StringType(), True),
        T.StructField("capacity", T.IntegerType(), True),
        T.StructField("peakCapacity", T.IntegerType(), True),
        T.StructField("activationDateFrom", T.TimestampType(), True),
        T.StructField("activationDateTo", T.TimestampType(), True),
        T.StructField("products", T.StringType(), True),
        T.StructField("CodSiglaProvincia", T.StringType(), True)
    ])
    
data_adding_rows = []

# Scorrimento per ogni coppia provincia-recapitista all'interno di Capacità simulate
for recap_prov in df_capacita_simulate.select('UNIFIED_DELIVERY_DRIVER','COD_SIGLA_PROVINCIA').distinct().collect():

    # Individuazione dei record per la coppia provincia-recapitista nella CAP capacities e nella capacità simulate
    df_capacita_simulate_recap_prov = df_capacita_simulate.filter((F.col('UNIFIED_DELIVERY_DRIVER')==recap_prov['UNIFIED_DELIVERY_DRIVER']) & 
                                                                  (F.col('COD_SIGLA_PROVINCIA')==recap_prov['COD_SIGLA_PROVINCIA']))
    list_capacita_simulate_recap_prov = df_capacita_simulate_recap_prov.collect()

    df_cap_capacities_prov_filtred = df_cap_capacities_prov.filter((F.col('unifiedDeliveryDriver')==recap_prov['UNIFIED_DELIVERY_DRIVER']) & 
                                                                   (F.col('CodSiglaProvincia')==recap_prov['COD_SIGLA_PROVINCIA'])).collect()

    # Per ogni coppia provincia-recapitista, osservazione dei flag
    conteggio_recap_prov = df_capacita_simulate_recap_prov.count()
    conteggio_recap_prov_false = df_capacita_simulate_recap_prov.filter(F.col('FLAG_DEFAULT')==False).count()
    conteggio_recap_prov_true = df_capacita_simulate_recap_prov.filter(F.col('FLAG_DEFAULT')==True).count()
  
    # CASO 1: Se tutti i flag sono true, tengo il record e scelgo la capacità di BAU
    if conteggio_recap_prov == conteggio_recap_prov_true:
        for row_cap in df_cap_capacities_prov_filtred:
            data_adding_row = {'unifiedDeliveryDriver': row_cap['unifiedDeliveryDriver'],
                                 'geoKey': row_cap['geoKey'],
                                 'capacity': row_cap['capacity'],
                                 'peakCapacity': row_cap['capacity'],
                                 'activationDateFrom': row_cap['activationDateFrom'],
                                 'activationDateTo': row_cap['activationDateTo'],
                                 'products': row_cap['products'],
                                 'CodSiglaProvincia': row_cap['CodSiglaProvincia']}
            
            data_adding_rows.append(data_adding_row)

    
    # CASO 2: Se tutti i flag sono false, tengo il record e scelgo la capacità di picco
    if conteggio_recap_prov == conteggio_recap_prov_false:
        for row_cap in df_cap_capacities_prov_filtred:
            data_adding_row = {'unifiedDeliveryDriver': row_cap['unifiedDeliveryDriver'],
                                 'geoKey': row_cap['geoKey'],
                                 'capacity': row_cap['peakCapacity'],
                                 'peakCapacity': row_cap['peakCapacity'],
                                 'activationDateFrom': row_cap['activationDateFrom'],
                                 'activationDateTo': row_cap['activationDateTo'],
                                 'products': row_cap['products'],
                                 'CodSiglaProvincia': row_cap['CodSiglaProvincia']}
            
            data_adding_rows.append(data_adding_row)

    # CASO 3: Se non tutti i flag sono uguali, splitto il record per tutte le settimane e scelgo la capacità di BAU per i true e picco per i false
    if conteggio_recap_prov != conteggio_recap_prov_false and conteggio_recap_prov != conteggio_recap_prov_true:
        
        # Aggiunta di un record con la corretta capacità per ogni CAP-settimana presente in CAP capacities, in base ad ogni settimana della capacità simulate
        for row_cap in df_cap_capacities_prov_filtred:
            
            for row_capsim in list_capacita_simulate_recap_prov:

                rows_type = row_capsim['FLAG_DEFAULT']
                data_adding_row = {'unifiedDeliveryDriver': row_cap['unifiedDeliveryDriver'],
                                     'geoKey': row_cap['geoKey'],
                                     # Se FLAG_DEFAULT è True aggiungo la capacità di BAU, altrimenti quella di picco
                                     'capacity': row_cap['capacity']*(rows_type==True) + row_cap['peakCapacity']*(rows_type==False),
                                     'peakCapacity': row_cap['capacity']*(rows_type==True) + row_cap['peakCapacity']*(rows_type==False),
                                     'activationDateFrom': row_capsim['ACTIVATION_DATE_FROM'],
                                     'activationDateTo': row_capsim['ACTIVATION_DATE_TO'],
                                     'products': row_cap['products'],
                                     'CodSiglaProvincia': row_cap['CodSiglaProvincia']}
                
                data_adding_rows.append(data_adding_row)

    
df_cap_capacities_final_stg = spark.createDataFrame(data_adding_rows, schema=schema_adding_rows)

# Per le coppie provincia-recapitista che non si trovano in Capacità simulate, lascio i record invariati in CAP capacities
df_cap_capacities_nocapsim = df_cap_capacities_prov.join(df_capacita_simulate,
                                        (df_cap_capacities_prov.CodSiglaProvincia==df_capacita_simulate.COD_SIGLA_PROVINCIA) & 
                                        (df_cap_capacities_prov.unifiedDeliveryDriver==df_capacita_simulate.UNIFIED_DELIVERY_DRIVER),'left')\
                                        .select(df_cap_capacities_prov['*'])\
                                        .filter(df_capacita_simulate['CAPACITY'].isNull())

df_cap_capacities_final = df_cap_capacities_final_stg.union(df_cap_capacities_nocapsim)

df_cap_capacities_final = df_cap_capacities_final.withColumnRenamed('unifiedDeliveryDriver','UNIFIED_DELIVERY_DRIVER')\
                                                 .withColumnRenamed('geoKey','GEOKEY')\
                                                 .withColumnRenamed('capacity','CAPACITY')\
                                                 .withColumnRenamed('peakCapacity','PEAK_CAPACITY')\
                                                 .withColumnRenamed('activationDateFrom','ACTIVATION_DATE_FROM')\
                                                 .withColumnRenamed('activationDateTo','ACTIVATION_DATE_TO')\
                                                 .withColumn('LAST_UPDATE_TIMESTAMP',F.current_timestamp())\
                                                 .withColumn('SIMULAZIONE_ID',F.lit('1'))\
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