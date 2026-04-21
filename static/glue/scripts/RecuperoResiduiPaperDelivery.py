import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','mese_simulazione','s3_bucket','secretsManager_SecretId','jdbc_connection'])

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
s3_bucket = args['s3_bucket']
secretsManager_SecretId = args['secretsManager_SecretId']
jdbc_connection = args['jdbc_connection']

# recupero parametro mese_simulazione (formato "yyyy-mm")
date = args['mese_simulazione']


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



print('Richiamo GET_PAPER_DELIVERY')
# Richiamo GET_PAPER_DELIVERY

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
  dict_response=ast.literal_eval(string_response)

  # Estrazione del body e poi degli items (posti come dizionari innestati)
  dict_response_body=ast.literal_eval(dict_response['body'])

  return dict_response_body
  
# Richiamo della lambda

schema_paperdel = T.StructType() \
      .add("attempt",T.StringType(),True) \
      .add("cap",T.StringType(),True) \
      .add("createdAt",T.StringType(),True) \
      .add("iun",T.StringType(),True) \
      .add("notificationSentAt",T.StringType(),True) \
      .add("pk",T.StringType(),True) \
      .add("prepareRequestDate",T.StringType(),True)\
      .add("priority",T.StringType(),True) \
      .add("productType",T.StringType(),True) \
      .add("province",T.StringType(),True) \
      .add("requestId",T.StringType(),True)\
      .add("senderPaId",T.StringType(),True)\
      .add("sk",T.StringType(),True)\
      .add("tenderId",T.StringType(),True)\
      .add("unifiedDeliveryDriver",T.StringType(),True)\
      .add("workflowStep",T.StringType(),True)


list_parameters=["pn-DelayerPaperDelivery", date, "EVALUATE_SENDER_LIMIT"]
flag=True
j=0

while flag==True:
    print(j)

    # Gestione della paginazione: se è la prima chiamata ometto il parametro aggiuntivo, altrimenti viene valutata la sua presenza per continuare la lettura 
    if j==0:
        dict_response_body_paperdel=lambda_to_dict(prov='',operationType='GET_PAPER_DELIVERY',list_parameters=list_parameters)
    else:
        list_parameters_v1=list_parameters+[adding_parameter]
        dict_response_body_paperdel=lambda_to_dict(prov='',operationType='GET_PAPER_DELIVERY',list_parameters=list_parameters_v1)

    # Se è presente il parametro aggiuntivo vado avanti con le chiamate della lambda, altrimenti finisco
    try:
        adding_parameter=dict_response_body_paperdel['lastEvaluatedKey']
    except:
        flag=False

    # Trasformazione della lista di dizionari in dataframe
    dict_response_items_paperdel=dict_response_body_paperdel['items']
    row_list=[]
    
    for diz in dict_response_items_paperdel:
        # Aggiungo le colonne mancanti allo schema target se assenti
        for col in ['priority','tenderId','unifiedDeliveryDriver']:
            try:
                col_exists=diz[col]
            except:
                diz[col]=''
        # Ordinamento
        diz_sorted=dict(sorted(diz.items()))
        # Riempimento lista delle righe
        row=list(diz_sorted.values())
        row_list.append(row)

    # Creazione dataframe totale
    if j==0:
        df_paperdel_tot=spark.createDataFrame(row_list,schema_paperdel)
    else:
        df_paperdel=spark.createDataFrame(row_list,schema_paperdel)
        df_paperdel_tot=df_paperdel_tot.union(df_paperdel)

    j=j+1

# Filtro per residui
df_paperdel_tot_filtred=df_paperdel_tot.filter(F.col('workflowStep')=='EVALUATE_SENDER_LIMIT')

id_timestamp=[["1"]]
timestamp_df=spark.createDataFrame(id_timestamp,["id"])

timestamp_df = timestamp_df.withColumn("current_timestamp_string",F.date_format(F.current_timestamp(), "yyyyMMdd"))

anno_corrente = timestamp_df.collect()[0][1][:4]
mese_corrente = timestamp_df.collect()[0][1][5:6]
giorno_corrente = timestamp_df.collect()[0][1][6:8]

path = "s3://"+s3_bucket+"/output/residui/" + anno_corrente + "/" \
                                                                  + mese_corrente + "/" \
                                                                  + giorno_corrente + "/" \
                                                                  + date
                                                                  

print('Export in S3')
# Export in S3
df_paperdel_tot_filtred.write.mode('overwrite').csv(path).option("header", True)

# da lasciare come ultimo comando per indicare che il job ha terminato con SUCCESS la sua esecuzione
job.commit()
