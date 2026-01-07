import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','id_simulazione_automatizzata','id_simulazione_manuale','lista_date','count_residui_ultima_settimana','s3_bucket','secretsManager_SecretId','jdbc_connection'])


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


#recupero parametri 'id_simulazione_automatizzata','id_simulazione_manuale','lista_date','count_residui_ultima_settimana' dalla step function
id_simulazione_automatizzata = args['id_simulazione_automatizzata'] # tipo: stringa
id_simulazione_manuale = args['id_simulazione_manuale'] # tipo: stringa
if id_simulazione_automatizzata != '-':
    id_simulazione = int(id_simulazione_automatizzata)
elif id_simulazione_manuale != '':
    id_simulazione = int(id_simulazione_manuale)
else:
    raise Exception("I parametri id_simulazione_automatizzata e id_simulazione_manuale sono entrambi nulli!")

lista_date = args['lista_date'] # tipo: lista, formato "yyyy-mm-dd"
lista_date = ast.literal_eval(lista_date)
count_residui_ultima_settimana = int(args['count_residui_ultima_settimana'])


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
      .add("week_delivery",T.StringType(),True)\
      .add("workflowStep",T.StringType(),True)


for data in lista_date:
    
    list_parameters=["pn-DelayerPaperDeliveryMock", data, "EVALUATE_PRINT_CAPACITY"]
    flag=True
    j=0
    
    while flag==True:
    
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
            # aggiunta data
            diz['week_delivery'] = data
            # Ordinamento
            diz_sorted=dict(sorted(diz.items()))
            # Riempimento lista delle righe
            row=list(diz_sorted.values())
            row_list.append(row)
            
    
        # Creazione dataframe totale
        if j==0 and data==lista_date[0]:
            df_paperdel_tot=spark.createDataFrame(row_list,schema_paperdel)
        else:
            df_paperdel=spark.createDataFrame(row_list,schema_paperdel)
            df_paperdel_tot=df_paperdel_tot.union(df_paperdel)

        j=j+1


print('Create Output DataFrame')
# Filtro per output
df_paperdel_tot_filtred=df_paperdel_tot.filter(F.col('workflowStep')=='EVALUATE_PRINT_CAPACITY')\
    .withColumn("SETTIMANA_DELIVERY",F.to_date(F.col("week_delivery"),"yyyy-MM-dd"))\
    .drop('week_delivery')

df_output_grafico_ente = df_paperdel_tot_filtred.groupBy(["senderPaId","SETTIMANA_DELIVERY"])\
    .agg(F.countDistinct('requestId'))\
    .withColumnRenamed("count(DISTINCT requestId)", "COUNT_REQUEST")\
    .withColumnRenamed("senderPaId", "SENDER_PA_ID")\
    .withColumn('SIMULAZIONE_ID',F.lit(id_simulazione))\
    .select("SIMULAZIONE_ID","SENDER_PA_ID","SETTIMANA_DELIVERY","COUNT_REQUEST")

df_output_grafico_reg_recap = df_paperdel_tot_filtred.join(df_cap_prov, df_paperdel_tot_filtred.province == df_cap_prov.COD_SIGLA_PROVINCIA)\
    .groupBy(["province","Regione","unifiedDeliveryDriver","SETTIMANA_DELIVERY"])\
    .agg(F.countDistinct('requestId'))\
    .withColumn('PROVINCIA_RECAPITISTA', 
                    F.concat(F.col('province'),F.lit(' - '), F.col('unifiedDeliveryDriver')))\
    .withColumnRenamed("count(DISTINCT requestId)", "COUNT_REQUEST")\
    .withColumnRenamed("province", "PROVINCE")\
    .withColumnRenamed("Regione", "REGIONE")\
    .withColumnRenamed("unifiedDeliveryDriver", "UNIFIED_DELIVERY_DRIVER")\
    .withColumn('SIMULAZIONE_ID',F.lit(id_simulazione))\
    .select("SIMULAZIONE_ID","PROVINCE","REGIONE","UNIFIED_DELIVERY_DRIVER","SETTIMANA_DELIVERY","PROVINCIA_RECAPITISTA","COUNT_REQUEST")
    

print('Export in DB')
db_table='public."OUTPUT_GRAFICO_ENTE"'

df_output_grafico_ente.write \
    .format("jdbc") \
    .option("url", jdbc_connection) \
    .option("dbtable", db_table) \
    .option("user", response_SecretString['username']) \
    .option("password", response_SecretString['password']) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()


db_table='public."OUTPUT_GRAFICO_REG_RECAP"'

df_output_grafico_reg_recap.write \
    .format("jdbc") \
    .option("url", jdbc_connection) \
    .option("dbtable", db_table) \
    .option("user", response_SecretString['username']) \
    .option("password", response_SecretString['password']) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

        
if count_residui_ultima_settimana > 0:
    data = lista_date[-1]
    list_parameters_res=["pn-DelayerPaperDeliveryMock", data, "EVALUATE_SENDER_LIMIT"]
    flag=True
    j=0
    
    while flag==True:
        print(j)
    
        # Gestione della paginazione: se è la prima chiamata ometto il parametro aggiuntivo, altrimenti viene valutata la sua presenza per continuare la lettura 
        if j==0:
            dict_response_body_paperdel_res=lambda_to_dict(prov='',operationType='GET_PAPER_DELIVERY',list_parameters=list_parameters_res)
        else:
            list_parameters_res_v1=list_parameters_res+[adding_parameter]
            dict_response_body_paperdel_res=lambda_to_dict(prov='',operationType='GET_PAPER_DELIVERY',list_parameters=list_parameters_res_v1)
    
        # Se è presente il parametro aggiuntivo vado avanti con le chiamate della lambda, altrimenti finisco
        try:
            adding_parameter=dict_response_body_paperdel_res['lastEvaluatedKey']
        except:
            flag=False
    
        dict_response_items_paperdel_res=dict_response_body_paperdel_res['items']
        row_list_res=[]
        for diz in dict_response_items_paperdel_res:
            # Aggiungo le colonne mancanti allo schema target se assenti
            for col in ['priority','tenderId','unifiedDeliveryDriver']:
                try:
                    col_exists=diz[col]
                except:
                    diz[col]=''
            # aggiunta data
            diz['week_delivery'] = data
            # Ordinamento
            diz_sorted=dict(sorted(diz.items()))
            # Riempimento lista delle righe
            row=list(diz_sorted.values())
            row_list_res.append(row)
    
        # Creazione dataframe totale
        if j==0 and data==lista_date[-1]:
            df_paperdel_res_tot=spark.createDataFrame(row_list_res,schema_paperdel)
        else:
            df_paperdel_res=spark.createDataFrame(row_list_res,schema_paperdel)
            df_paperdel_res_tot=df_paperdel_res_tot.union(df_paperdel_res)
        
        j=j+1


    prima_data = lista_date[0]
    anno_riferimento = prima_data[:4]
    mese_riferimento = prima_data[5:7]
    
    path = "s3://"+s3_bucket+"/output/risultati/" + anno_riferimento + "/" \
                                                                        + mese_riferimento + "/residui/" \
                                                                        + "id" + str(id_simulazione) + "_" + data
    
    print('Export in S3')
    # Export in S3
    df_paperdel_res_tot.repartition(1).write.mode('overwrite').option("header",True).csv(path)


# id_timestamp=[["1"]]
# timestamp_df=spark.createDataFrame(id_timestamp,["id"])

# timestamp_df = timestamp_df.withColumn("current_timestamp_string",F.date_format(F.current_timestamp(), "yyyyMMddHHmmss"))
# timestamp_corrente = timestamp_df.collect()[0][1]




# da lasciare come ultimo comando per indicare che il job ha terminato con SUCCESS la sua esecuzione
job.commit()
