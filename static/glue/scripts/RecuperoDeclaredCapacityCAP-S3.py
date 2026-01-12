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
import calendar
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import datetime as dt
import math

# recupero parametri d'ambiente del job
secretsManager_SecretId = args['secretsManager_SecretId']
jdbc_connection = args['jdbc_connection']
s3_bucket = args['s3_bucket']

print('Import della tabella con i dati demografici di CAP e province')
######################################
# Import della tabella con i dati demografici di CAP e province

# recupero credenziali db da secretsmanager
client = boto3.client("secretsmanager")
response = client.get_secret_value(SecretId=secretsManager_SecretId)
response_SecretString = json.loads(response['SecretString'])

db_table = 'public."CAP_PROV_REG"'

df_cap_prov = spark.read \
    .format("jdbc") \
    .option("url", jdbc_connection) \
    .option("dbtable", db_table) \
    .option("user", response_SecretString['username']) \
    .option("password", response_SecretString['password']) \
    .option("driver", "org.postgresql.Driver") \
    .load()

print('prima lettura')
df_cap_prov.show()

df_cap=df_cap_prov.select('CAP').distinct()
lista_cap=[row['CAP'] for row in df_cap.collect()]


print('Richiamo lambda GET_DELCARED_CAPACITY')
#######################################
# Richiamo lambda GET_DELCARED_CAPACITY  

# Inizializzazione per il richiamo delle lambda
lambda_delayer=boto3.client('lambda')

# Funzione per creare la lista delle righe a partire dal richiamo della lambda

def lambda_to_dict(cap,operationType,list_parameters):
  payload={
  "operationType": operationType,
  "parameters": list_parameters}
  response=lambda_delayer.invoke(FunctionName='pn-testDelayerLambda',Payload=json.dumps(payload))

  # Lettura e decodifica dell'output della lambda
  read_response=response['Payload'].read()
  string_response=read_response.decode('utf-8')

  # Trasformazione da stringa in dizionario
  dict_response=ast.literal_eval(string_response)

  # Estrazione del body e poi degli items (posti come dizionari innestati)
  dict_response_body=ast.literal_eval(dict_response['body'])

  return dict_response_body


# Estrazione delle settimane a partire dalla data in input
data_simulazione=args['mese_simulazione']
id_simulazione_manuale=args['id_simulazione_manuale']

# Richiamo della lambda per province e settimane
schema_capacity = T.StructType() \
      .add("ACTIVATION_DATE_FROM",T.StringType(),True) \
      .add("ACTIVATION_DATE_TO",T.StringType(),True) \
      .add("PRODUCTS",T.StringType(),True)\
      .add("CAPACITY",T.IntegerType(),True) \
      .add("CREATED_AT",T.StringType(),True) \
      .add("GEOKEY",T.StringType(),True) \
      .add("PEAK_CAPACITY",T.IntegerType(),True) \
      .add("PK",T.StringType(),True)\
      .add("TENDER_ID",T.StringType(),True) \
      .add("TENDER_ID_GEOKEY",T.StringType(),True) \
      .add("UNIFIED_DELIVERY_DRIVER",T.StringType(),True)   
        

print(lista_cap)
row_list=[]
for cap in lista_cap:
    print(cap)
    dict_response_body_capacity=lambda_to_dict(cap=cap,operationType='GET_DECLARED_CAPACITY',list_parameters=["pn-PaperDeliveryDriverCapacities",cap,data_simulazione])

    # Trasformazione della lista di dizionari in dataframe
    dict_response_items_capacity=dict_response_body_capacity['items']
    

    for diz in dict_response_items_capacity:
        diz['ACTIVATION_DATE_FROM']=diz['activationDateFrom'][:11]+'00'+diz['activationDateFrom'][13:]
        del diz['activationDateFrom']
        try:
            test=diz['activationDateTo']
            diz['ACTIVATION_DATE_TO']=test
        except:
            diz['ACTIVATION_DATE_TO']=None
        diz['PRODUCTS'] = None
        # Ordinamento
        diz_sorted=dict(sorted(diz.items()))
        # Riempimento lista delle righe
        row=list(diz_sorted.values())
        row_list.append(row)
        
df_capacity_tot=spark.createDataFrame(row_list,schema_capacity)


df_capacity_tot=df_capacity_tot.withColumn('activationDateFrom',F.to_timestamp(F.col('ACTIVATION_DATE_FROM'),"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))\
                               .withColumn('activationDateTo',F.to_timestamp(F.col('ACTIVATION_DATE_TO'),"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))\
                               .withColumnRenamed( "UNIFIED_DELIVERY_DRIVER", "unifiedDeliveryDriver")\
                               .withColumnRenamed("GEOKEY", "geoKey")\
                               .withColumnRenamed("CAPACITY", "capacity")\
                               .withColumnRenamed("PEAK_CAPACITY", "peakCapacity")\
                               .withColumnRenamed("PRODUCTS", "products")\
                               .select('unifiedDeliveryDriver','geoKey','capacity','peakCapacity','activationDateFrom','activationDateTo','products')



print('Export in S3')
path = "s3://"+s3_bucket+"/input/cap_capacities/id"+id_simulazione_manuale+"_"+data_simulazione
max_rows = 10000
num_rows=df_capacity_tot.count()
print(num_rows)
part=math.ceil(num_rows/max_rows)
df_capacity_tot.repartition(part).write.mode('overwrite').option('header',True).option('sep',';').option('quoteAll','False').format('csv').save(path)



# da lasciare come ultimo comando per indicare che il job ha terminato con SUCCESS la sua esecuzione
job.commit()
