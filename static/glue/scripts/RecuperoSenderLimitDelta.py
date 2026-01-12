import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','mese_simulazione','secretsManager_SecretId','jdbc_connection'])

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

# recupero parametri d'ambiente del job
secretsManager_SecretId = args['secretsManager_SecretId']
jdbc_connection = args['jdbc_connection']


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

df_province=df_cap_prov.select('COD_SIGLA_PROVINCIA').distinct()
lista_province=[row['COD_SIGLA_PROVINCIA'] for row in df_province.collect()]

print('Richiamo lambda GET_SENDER_LIMIT')
#######################################
# Richiamo lambda GET_SENDER_LIMIT  

# Inizializzazione per il richiamo delle lambda
lambda_delayer=boto3.client('lambda')

# Funzione per creare la lista delle righe a partire dal richiamo della lambda

def lambda_to_dict(prov,operationType,list_parameters):
  payload={
  "operationType": operationType,
  "parameters": list_parameters}
  response=lambda_delayer.invoke(FunctionName='pn-testDelayerLambda',Payload=json.dumps(payload))

  # Lettura e decodifica dell'output della lambda
  read_response=response['Payload'].read()
  string_response=read_response.decode('utf-8')

  # Trasformazione da stringa in dizionario
  dict_response=json.loads(string_response)

  # Estrazione del body e poi degli items (posti come dizionari innestati)
  dict_response_body=json.loads(dict_response['body'])

  return dict_response_body

# Richiamo della lambda per provincia

schema_senderlim = T.StructType() \
      .add("DELIVERY_DATE",T.StringType(),True) \
      .add("fileKey",T.StringType(),True) \
      .add("MONTHLY_ESTIMATE",T.IntegerType(),True) \
      .add("ORIGINAL_ESTIMATE",T.IntegerType(),True) \
      .add("PA_ID",T.StringType(),True) \
      .add("PK",T.StringType(),True) \
      .add("PRODUCT_TYPE",T.StringType(),True)\
      .add("PROVINCE",T.StringType(),True) \
      .add("ttl",T.StringType(),True) \
      .add("WEEKLY_ESTIMATE",T.IntegerType(),True)

date = args['mese_simulazione']

for prov in lista_province:
    print(prov)
    
    list_parameters=[date,prov]
    flag=True
    j=0

    while flag==True:
        print(j)

        # Gestione della paginazione: se è la prima chiamata ometto il parametro aggiuntivo, altrimenti viene valutata la sua presenza per continuare la lettura 
        if j==0:
            dict_response_body_senderlim=lambda_to_dict(prov=prov,operationType='GET_SENDER_LIMIT',list_parameters=list_parameters)
        else:
            list_parameters_v1=list_parameters+[adding_parameter]
            dict_response_body_senderlim=lambda_to_dict(prov=prov,operationType='GET_SENDER_LIMIT',list_parameters=list_parameters_v1)
    
        # Se è presente il parametro aggiuntivo vado avanti con le chiamate della lambda, altrimenti finisco
        try:
            adding_parameter=dict_response_body_senderlim['lastEvaluatedKey']
        except:
            flag=False

        # Trasformazione della lista di dizionari in dataframe
        dict_response_items_senderlim=dict_response_body_senderlim['items']
        row_list=[]

        for diz in dict_response_items_senderlim:
            # Conversione a interi dei valori decimali
            diz['monthlyEstimate']=int(round(diz['monthlyEstimate'],0))
            # Ordinamento
            diz_sorted=dict(sorted(diz.items()))
            # Riempimento lista delle righe
            row=list(diz_sorted.values())
            row_list.append(row)

        # Creazione dataframe provincia e append al dataframe totale
        if prov==lista_province[0] and j==0:
            df_senderlim_tot=spark.createDataFrame(row_list,schema_senderlim)
        else:
            df_senderlim=spark.createDataFrame(row_list,schema_senderlim)
            df_senderlim_tot=df_senderlim_tot.union(df_senderlim)
 
        j=j+1


df_senderlim_tot=df_senderlim_tot.withColumn('DELIVERY_DATE',F.col('DELIVERY_DATE').cast(T.DateType()))\
                                 .withColumn('LAST_UPDATE_TIMESTAMP',F.current_timestamp())\
                                 .select(['PK','DELIVERY_DATE','WEEKLY_ESTIMATE','MONTHLY_ESTIMATE','ORIGINAL_ESTIMATE','PA_ID','PRODUCT_TYPE','PROVINCE','LAST_UPDATE_TIMESTAMP'])
   
     
print('Export su DB')      
# Export su DB

db_table='public."SENDER_LIMIT_DELTA"'

df_senderlim_tot.write \
    .format("jdbc") \
    .option("url", jdbc_connection) \
    .option("dbtable", db_table) \
    .option("user", response_SecretString['username']) \
    .option("password", response_SecretString['password']) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()
    




###############################
df_read = spark.read \
    .format("jdbc") \
    .option("url", jdbc_connection) \
    .option("dbtable", db_table) \
    .option("user", response_SecretString['username']) \
    .option("password", response_SecretString['password']) \
    .option("driver", "org.postgresql.Driver") \
    .load()
    
    
df_read.show()

# da lasciare come ultimo comando per indicare che il job ha terminato con SUCCESS la sua esecuzione
job.commit()
