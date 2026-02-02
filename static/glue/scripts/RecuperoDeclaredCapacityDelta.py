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
import calendar
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import datetime as dt

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


print('Richiamo lambda GET_DELCARED_CAPACITY')
#######################################
# Richiamo lambda GET_DELCARED_CAPACITY  

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
  dict_response=ast.literal_eval(string_response)

  # Estrazione del body e poi degli items (posti come dizionari innestati)
  dict_response_body=ast.literal_eval(dict_response['body'])

  return dict_response_body


# Estrazione delle settimane a partire dalla data in input
date_input=args['mese_simulazione']
date_input_dtfmt=datetime.strptime(date_input,'%Y-%m-%d').date()
anno=date_input_dtfmt.year
mese=date_input_dtfmt.month
calendario=calendar.Calendar()
# Seleziono solamente i lunedì futuri al giorno in questione
date_list=[day for day in calendario.itermonthdates(anno,mese) if day.weekday()==0 and day.month==mese and day>=date_input_dtfmt]
# Prendo anche il primo lunedi del mese successivo
date_list.append(date_list[len(date_list)-1]+dt.timedelta(days=7))
for i in range(0,len(date_list)):
    date_list[i]=date_list[i].strftime('%Y-%m-%d')
print(date_list)


# Richiamo della lambda per province e settimane
schema_capacity = T.StructType() \
      .add("ACTIVATION_DATE_TO",T.StringType(),True) \
      .add("PRODUCT_890",T.BooleanType(),True)\
      .add("PRODUCT_AR",T.BooleanType(),True)\
      .add("PRODUCT_RS",T.BooleanType(),True)\
      .add("ACTIVATION_DATE_FROM",T.StringType(),True) \
      .add("CAPACITY",T.IntegerType(),True) \
      .add("CREATED_AT",T.StringType(),True) \
      .add("GEOKEY",T.StringType(),True) \
      .add("PEAK_CAPACITY",T.IntegerType(),True) \
      .add("PK",T.StringType(),True)\
      .add("TENDER_ID",T.StringType(),True) \
      .add("TENDER_ID_GEOKEY",T.StringType(),True) \
      .add("UNIFIED_DELIVERY_DRIVER",T.StringType(),True)   
        

products=['890', 'AR', 'RS']


for date in date_list:

    for prov in lista_province:
        
        dict_response_body_capacity=lambda_to_dict(prov=prov,operationType='GET_DECLARED_CAPACITY',list_parameters=["pn-PaperDeliveryDriverCapacities",prov,date])

        # Trasformazione della lista di dizionari in dataframe
        dict_response_items_capacity=dict_response_body_capacity['items']
        row_list=[]

        for diz in dict_response_items_capacity:
            # Valorizzazione data di partenza e di fine
            diz['activationDateFrom']=date+' 00:00:00'
            try:
                test=diz['activationDateTo']
                date_stg=datetime.strptime(diz['activationDateFrom'],"%Y-%m-%d %H:%M:%S")
                date_stg2=date_stg+timedelta(seconds=604799)
                diz['ACTIVATION_DATE_TO']=date_stg2.strftime("%Y-%m-%d %H:%M:%S")
                del diz['activationDateTo']
            except:
                diz['ACTIVATION_DATE_TO']=None
            # Aggiunta colonna per prodotto
            for product in products:
                diz["PRODUCT_"+product]=False
            # Valorizzazione in caso di presenza del prodotto nella riga
            for product in diz['products']:
                diz['PRODUCT_'+product]=True
            # Eliminazione del campo unico prodotti
            del diz['products']
            # Ordinamento
            diz_sorted=dict(sorted(diz.items()))
            # Riempimento lista delle righe
            row=list(diz_sorted.values())
            row_list.append(row)

        # Creazione dataframe provincia e append al dataframe totale
        if prov==lista_province[0] and date==date_list[0]:
            df_capacity_tot=spark.createDataFrame(row_list,schema_capacity)
        else:
            df_capacity=spark.createDataFrame(row_list,schema_capacity)
            df_capacity_tot=df_capacity_tot.union(df_capacity)


df_capacity_tot=df_capacity_tot.withColumn('ACTIVATION_DATE_FROM',F.col('ACTIVATION_DATE_FROM').cast(T.TimestampType()))\
                               .withColumn('ACTIVATION_DATE_TO',F.col('ACTIVATION_DATE_TO').cast(T.TimestampType()))\
                               .withColumn('CREATED_AT',F.col('CREATED_AT').cast(T.TimestampType()))\
                               .withColumn('LAST_UPDATE_TIMESTAMP',F.current_timestamp())


# Deduplicazione delle righe con conseguente popolamento del campo PRODUCTION_CAPACITY
# Assunzione: ogni pk può avere al più 2 record di cui uno con ACTIVATION_DATE_TO valorizzata e uno con ACTIVATION_DATE_TO nulla

df_capacity_tot.filter(F.col('GEOKEY')==lista_province[0]).show()

# Raggruppamento tenendo la capacità massima per i due record
df_capacity_tot_stg=df_capacity_tot.groupBy('PK', 'ACTIVATION_DATE_FROM').agg(F.max(F.col('CAPACITY')).alias('MAX_CAPACITY'), F.max_by(F.col('ACTIVATION_DATE_TO'),F.col('CAPACITY')).alias('MAX_DATE'))

# Aggiunta del campo di capacità massima al dataframe
df_capacity_tot_stg2=df_capacity_tot.join(df_capacity_tot_stg, on=['PK','ACTIVATION_DATE_FROM'], how='left')

# Deduplicazione eliminando i record con ACTIVATION_DATE_TO valorizzata
df_capacity_tot_dedup=df_capacity_tot_stg2.filter(F.col('ACTIVATION_DATE_TO').isNull())

# Valorizzazione dei campi PRODUCTION_CAPACITY e ACTIVATION_DATE_TO
df_capacity_tot_final=df_capacity_tot_dedup.withColumn('PRODUCTION_CAPACITY',F.when(F.col('CAPACITY')==F.col('MAX_CAPACITY'),0).otherwise(F.col('MAX_CAPACITY')))\
                                           .withColumn('ACTIVATION_DATE_TO',F.expr('ACTIVATION_DATE_FROM + INTERVAL 6 DAYS'))\
                                           .select(['CAPACITY','GEOKEY','TENDER_ID_GEOKEY','PRODUCT_890','PRODUCT_AR','PRODUCT_RS','TENDER_ID','UNIFIED_DELIVERY_DRIVER',
                                                   'CREATED_AT','PEAK_CAPACITY','ACTIVATION_DATE_FROM','ACTIVATION_DATE_TO','PK','PRODUCTION_CAPACITY','LAST_UPDATE_TIMESTAMP'])


df_capacity_tot_final.filter(F.col('GEOKEY')==lista_province[0]).show()

print('Export su DB')      
# Export su DB

db_table='public."DECLARED_CAPACITY_DELTA"'

df_capacity_tot_final.write \
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
