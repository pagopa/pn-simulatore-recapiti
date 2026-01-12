import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','mese_simulazione','s3_bucket','secretsManager_SecretId','jdbc_connection'])
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])
# mese_simulazione = '2025-10-06'

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

##################################
import boto3
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window
import ast
import json
import calendar
from datetime import date, timedelta
import math

# recupero parametri d'ambiente del job
s3_bucket = args['s3_bucket']
secretsManager_SecretId = args['secretsManager_SecretId']
jdbc_connection = args['jdbc_connection']

max_rows = 10000

print('Import della tabella con i dati demografici di CAP e province')
######################################
# Import della tabella con i dati demografici di CAP e province

# recupero credenziali db da secretsmanager
client = boto3.client("secretsmanager")
response = client.get_secret_value(SecretId=secretsManager_SecretId)
response_SecretString = json.loads(response['SecretString'])

print('Lettura CAP_PROV_REG')

db_table = 'public."CAP_PROV_REG"'

df_cap_prov = spark.read \
    .format("jdbc") \
    .option("url", jdbc_connection) \
    .option("dbtable", db_table) \
    .option("user", response_SecretString['username']) \
    .option("password", response_SecretString['password']) \
    .option("driver", "org.postgresql.Driver") \
    .load()


df_cap_prov.show()

###########

db_table = 'public."SENDER_LIMIT"'

df_senderlim_tot = spark.read \
    .format("jdbc") \
    .option("url", jdbc_connection) \
    .option("dbtable", db_table) \
    .option("user", response_SecretString['username']) \
    .option("password", response_SecretString['password']) \
    .option("driver", "org.postgresql.Driver") \
    .load()

print('Lettura SENDER_LIMIT')
df_senderlim_tot.show()

df_senderlim_tot = df_senderlim_tot.drop('ID')

# Lista delle province
df_province=df_cap_prov.select('COD_SIGLA_PROVINCIA').distinct()
lista_province=[row['COD_SIGLA_PROVINCIA'] for row in df_province.collect()]


# Calcolo RS su totale della monthly provincia-ente (aggregando per prodotto)
print('Calcolo RS')

df_senderlim_tot_grouped=df_senderlim_tot.groupBy('PROVINCE','PA_ID').agg(F.sum('MONTHLY_ESTIMATE').alias('SUM_MONTHLY_ESTIMATE'),
                                                                         F.max_by(F.col('DELIVERY_DATE'),F.col('PA_ID')).alias('DELIVERY_DATE'))

df_senderlim_tot_rs_stg=df_senderlim_tot_grouped.withColumn('MONTHLY_ESTIMATE',F.floor(F.col('SUM_MONTHLY_ESTIMATE')/50))\
                                                .withColumn('PK',F.concat(F.col('PA_ID'),F.lit('~'),F.col('PROVINCE'),F.lit('~RS')))\
                                                .withColumn('WEEKLY_ESTIMATE',F.lit(0))\
                                                .withColumn('ORIGINAL_ESTIMATE',F.lit(0))\
                                                .withColumn('PRODUCT_TYPE',F.lit('RS'))\
                                                .withColumn('LAST_UPDATE_TIMESTAMP',F.current_timestamp())\
                                                .select(['PK','DELIVERY_DATE','WEEKLY_ESTIMATE','MONTHLY_ESTIMATE','ORIGINAL_ESTIMATE',
                                                         'PA_ID','PRODUCT_TYPE','PROVINCE','LAST_UPDATE_TIMESTAMP'])

df_senderlim_tot_rs=df_senderlim_tot.union(df_senderlim_tot_rs_stg)
    
# Estrazione della lista di dizionari corrispondente al dataframe
print('Creazione dizionario ed estrazione calendario')

row_list=df_senderlim_tot_rs.collect()
dict_response_senderlim_items=[row.asDict() for row in row_list]

# Estrazione del calendario mensile per settimana e del numero di giorni nel mese
anno = int(args['mese_simulazione'][:4])
mese = int(args['mese_simulazione'][5:7])

# anno = int(mese_simulazione[:4])
# mese = int(mese_simulazione[5:7])
print(anno+mese)

settimane = calendar.monthcalendar(anno, mese)
giorni_mese=sum(len([i for i in settimana if i!=0]) for settimana in settimane)


print('Estrazione dei lunedì')
# Creazione della lista dei lunedì delle settimane che includono giorni del mese corrente
from datetime import date, timedelta

calendario=calendar.Calendar()
lista_lunedi=[day for day in calendario.itermonthdates(anno,mese) if day.weekday()==0]

# Non devono essere considerati giorni fuori dal mese corrente
if lista_lunedi[0]!=mese:
    lista_lunedi[0]=date(anno,mese,1)
    
print('lista_lunedi:',lista_lunedi)

print ('Calcolo postalizzazioni settimanali')
# Calcolo postalizzazioni

row_list=[]
col_list=['pk','COD_SIGLA_PROVINCIA','productType','paId','CreatedAt','Postalizzazioni','attempt']

for item in dict_response_senderlim_items:
    
    # Calcolo postalizzazioni giornaliere
    postalizzazioni_daily=item['MONTHLY_ESTIMATE']/giorni_mese
    postalizzazioni_weekly_list=[]
    
    # Calcolo postalizzazioni settimanali
    somma_postalizzazioni=0
    for settimana in settimane[:-1]:
      giorni_settimana=len([i for i in settimana if i!=0])
      postalizzazioni_weekly=int(round(postalizzazioni_daily*giorni_settimana,0))
      postalizzazioni_weekly_list.append(postalizzazioni_weekly)
      somma_postalizzazioni=somma_postalizzazioni+postalizzazioni_weekly
    
    # Aggiunta delle postalizzazioni avanzate all'ultima settimana
    last_week=item['MONTHLY_ESTIMATE']-somma_postalizzazioni
    postalizzazioni_weekly_list.append(last_week)

    # Dataframe delle postalizzazioni raggruppate per settimana e provincia
    for attempt in [0,1]:
        for i in range(len(lista_lunedi)):
            row=[]
            row.append(item['PA_ID']+'~'+item['PRODUCT_TYPE']+'~'+item['PROVINCE'])
            row.append(item['PROVINCE'])
            row.append(item['PRODUCT_TYPE'])
            row.append(item['PA_ID'])
            row.append(lista_lunedi[i])
            if attempt==0:
                row.append(postalizzazioni_weekly_list[i])
            # In caso di secondo tentativo aggiungo un valore pari al 5% dell'originale
            if attempt==1:
                row.append(math.floor(postalizzazioni_weekly_list[i]/20))
            row.append(attempt)
            row_list.append(row)

df_postalizzazioni=spark.createDataFrame(row_list,col_list).filter(F.col('Postalizzazioni')>0)    


print('Split postalizzazioni per CAP')
# Suddivisione delle postalizzazioni settimanali per CAP
df_postalizzazioni_cap=df_postalizzazioni.join(df_cap_prov,on='COD_SIGLA_PROVINCIA',how='left')\
                                         .withColumn("quota_reale", F.col('Postalizzazioni') * F.col('PERCENTUALE_POP_CAP'))\
                                         .withColumn("floor_val", F.floor(F.col("quota_reale")))\
                                         .withColumn("resto", F.col("quota_reale") - F.col("floor_val"))\
                                         .select('COD_SIGLA_PROVINCIA','pk','productType','paId','CreatedAt','Postalizzazioni','attempt','CAP',                                                'PERCENTUALE_POP_CAP','quota_reale','floor_val','resto')

df_postalizzazioni_cap.show()

# Procediamo separatamente per primi e secondi tentativi
df_postalizzazioni_cap_1att=df_postalizzazioni_cap.filter(F.col('attempt')==0)
df_postalizzazioni_cap_2att=df_postalizzazioni_cap.filter(F.col('attempt')==1)

print('Gestione dei resti')
# Calcolo il numero di postalizzazioni per CAP e ridistribuisco i resti per attempt 1
column_list = ['pk','CreatedAt']
win_spec = Window.partitionBy([F.col(x) for x in column_list])

df_postalizzazioni_mancanti_1att = df_postalizzazioni_cap_1att.withColumn("somma_floor", F.sum("floor_val").over(win_spec))\
                                                            .withColumn("totale_gruppo", F.max('Postalizzazioni').over(win_spec))\
                                                            .withColumn("manca", F.col("totale_gruppo") - F.col("somma_floor"))
                                                            
w_group_desc_resto = Window.partitionBy([F.col(x) for x in column_list]).orderBy(F.col("resto").desc())

df_ranking_1att = df_postalizzazioni_mancanti_1att.withColumn("rank_resti", F.row_number().over(w_group_desc_resto))

df_postalizzazioni_cap_final_1att = df_ranking_1att.withColumn(
        "Postalizzazioni_cap",
        F.col("floor_val") +
        F.when(F.col("rank_resti") <= F.col("manca"), 1).otherwise(0)
    )


# Calcolo il numero di postalizzazioni per CAP e ridistribuisco i resti per attempt 2
df_postalizzazioni_mancanti_2att = df_postalizzazioni_cap_2att.withColumn("somma_floor", F.sum("floor_val").over(win_spec))\
                                                            .withColumn("totale_gruppo", F.max('Postalizzazioni').over(win_spec))\
                                                            .withColumn("manca", F.col("totale_gruppo") - F.col("somma_floor"))
                                                            

df_ranking_2att = df_postalizzazioni_mancanti_2att.withColumn("rank_resti", F.row_number().over(w_group_desc_resto))

df_postalizzazioni_cap_final_2att = df_ranking_2att.withColumn(
        "Postalizzazioni_cap",
        F.col("floor_val") +
        F.when(F.col("rank_resti") <= F.col("manca"), 1).otherwise(0)
    )

# Unisco i due dataset degli attempt
df_postalizzazioni_cap_final=df_postalizzazioni_cap_final_1att.union(df_postalizzazioni_cap_final_2att)\
                                                              .filter(F.col('Postalizzazioni_cap')!=0)\
                                                              .withColumn('Postalizzazioni_cap',F.col('Postalizzazioni_cap').cast(T.IntegerType()))

df_postalizzazioni_cap_final.show()

print('Esplosione del dataframe')
# 'Esplosione' del dataframe in più righe quante sono le postalizzazioni
df_postalizzazioni_exploded=df_postalizzazioni_cap_final.withColumn('array_rep',F.array_repeat(F.lit(None),df_postalizzazioni_cap_final['Postalizzazioni_cap']))\
                                                     .withColumn('array_rep_v1',F.explode('array_rep'))


df_postalizzazioni_final=df_postalizzazioni_exploded.withColumn('iun',F.monotonically_increasing_id()+1000000)\
                                                    .withColumn('prepareRequestDate',F.to_timestamp(F.col('CreatedAt'),"yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'"))\
                                                    .withColumn('workflowStep',F.lit('EVALUATE_SENDER_LIMIT'))\
                                                    .withColumn('NotificationSentAt',F.to_timestamp(F.col('CreatedAt'),"yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'"))\
                                                    .withColumn('RequestID',F.monotonically_increasing_id()+1000000)\
                                                    .withColumnRenamed('paId','SenderPaId')\
                                                    .withColumnRenamed('COD_SIGLA_PROVINCIA','province')\
                                                    .select('requestId','notificationSentAt','prepareRequestDate','productType','senderPaId','province','cap','attempt','iun')

print('Export in S3')
# Export in csv a lotti di 10.000 righe

#calcolo data parametro per import
if lista_lunedi[0]!=mese:
    del lista_lunedi[0]

lunedì_mese_successivo = lista_lunedi[-1] + timedelta(days=7)
lista_lunedi.append(lunedì_mese_successivo)  

print('lista_lunedi:',lista_lunedi)


id_timestamp=[["1"]]
timestamp_df=spark.createDataFrame(id_timestamp,["id"])

timestamp_df = timestamp_df.withColumn("current_timestamp_string",F.date_format(F.current_timestamp(), "yyyyMMdd"))

anno_corrente = timestamp_df.collect()[0][1][:4]
mese_corrente = timestamp_df.collect()[0][1][4:6]
giorno_corrente = timestamp_df.collect()[0][1][6:8]

path = "s3://"+s3_bucket+"/input/"  + anno_corrente + "/" \
                                                          + mese_corrente + "/" \
                                                          + giorno_corrente + "/" \
                                                          + str(anno) + "-" + str(mese)

#suddivisione dataset in settimane
for lunedi in lista_lunedi:
    if lunedi == lista_lunedi[0]:
        df_split = df_postalizzazioni_final.filter(F.col('prepareRequestDate') < lunedi )
    else:
        df_split = df_postalizzazioni_final.filter((F.col('prepareRequestDate') < lunedi) & (F.col('prepareRequestDate') >= (lunedi + timedelta(days=-7))))
    df_split.show(1)
    num_rows=df_split.count()
    part=math.ceil(num_rows/max_rows)
    df_split.repartition(part).write.mode('overwrite').option('header',True).option('sep',';').option('quoteAll','true').format('csv').save(path + "/" + str(lunedi))
    

# da lasciare come ultimo comando per indicare che il job ha terminato con SUCCESS la sua esecuzione
job.commit()
