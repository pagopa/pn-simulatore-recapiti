import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','mese_simulazione','id_simulazione_manuale','s3_bucket','secretsManager_SecretId','jdbc_connection'])
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
id_simulazione = int(args['id_simulazione_manuale']) # tipo: stringa
max_rows = 10000

print('Import della tabella con i dati demografici di CAP e province')
######################################
# Import della tabella con i dati demografici di CAP e province

#recupero credenziali db da secretsmanager
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

db_table = 'public."SENDER_LIMIT_MOCK"'

df_senderlim_mock = spark.read \
    .format("jdbc") \
    .option("url", jdbc_connection) \
    .option("dbtable", db_table) \
    .option("user", response_SecretString['username']) \
    .option("password", response_SecretString['password']) \
    .option("driver", "org.postgresql.Driver") \
    .load()

print('Lettura SENDER_LIMIT_MOCK')
df_senderlim_mock.show()

df_senderlim_mock = df_senderlim_mock.drop('ID')\
                                     .filter(F.col('SIMULAZIONE_ID')==id_simulazione)


# Lavoro solamente se ci sono commesse di mock
if df_senderlim_mock.count()>0:
        
    # Estrazione della lista di dizionari corrispondente al dataframe
    print('Creazione dizionario ed estrazione calendario')
    
    row_list=df_senderlim_mock.collect()
    dict_response_senderlim_items=[row.asDict() for row in row_list]
    
    # Estrazione del calendario mensile per settimana e del numero di giorni nel mese
    anno = int(args['mese_simulazione'][:4])
    mese = int(args['mese_simulazione'][5:7])
    
    print(anno+mese)
    
    settimane = calendar.monthcalendar(anno, mese)
    giorni_mese=sum(len([i for i in settimana if i!=0]) for settimana in settimane)
    
    
    print('Estrazione dei lunedì')
    # Creazione della lista dei lunedì delle settimane che includono giorni del mese corrente
    calendario=calendar.Calendar()
    lista_lunedi=[day for day in calendario.itermonthdates(anno,mese) if day.weekday()==0]
    
    # Non devono essere considerati giorni fuori dal mese corrente
    if lista_lunedi[0]!=mese:
        lista_lunedi[0]=date(anno,mese,1)
        
    print('lista_lunedi:',lista_lunedi)
    
    print ('Calcolo postalizzazioni settimanali')
    # Calcolo postalizzazioni
    
    row_list=[]
    col_list=['SUDDIVISIONE_GEOGRAFICA','PRODUCT_TYPE','PA_ID','DELIVERY_DATE','Postalizzazioni']
    
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
    
        # Dataframe delle postalizzazioni raggruppate per settimana e provincia/regione/tutta Italia
        for i in range(len(lista_lunedi)):
            row=[]
            row.append(item['SUDDIVISIONE_GEOGRAFICA'])
            row.append(item['PRODUCT_TYPE'])
            row.append(item['PA_ID'])
            row.append(lista_lunedi[i])
            row.append(postalizzazioni_weekly_list[i])
            row_list.append(row)
    
    df_postalizzazioni=spark.createDataFrame(row_list,col_list).filter(F.col('Postalizzazioni')>0)   
    
    
    print('Split postalizzazioni per CAP sul territorio nazionale')
    # Lavorazione per suddivisione geografica nazionale
    w_naz = Window.partitionBy()
    df_cap_prov_prop_naz=df_cap_prov.withColumn('PROP_POP_CAP_NAZ',F.col('POP_CAP')/F.sum(F.col('POP_CAP')).over(w_naz))\
                                            .withColumn('SUDDIVISIONE_NAZIONALE',F.lit('Italia'))
    
    # Suddivisione delle postalizzazioni settimanali per CAP
    df_postalizzazioni_naz=df_postalizzazioni.filter(F.col('SUDDIVISIONE_GEOGRAFICA')=='Italia')
    
    df_postalizzazioni_cap_naz=df_postalizzazioni_naz.join(df_cap_prov_prop_naz,df_postalizzazioni.SUDDIVISIONE_GEOGRAFICA==df_cap_prov_prop_naz.SUDDIVISIONE_NAZIONALE,how='left')\
                                                     .withColumn("quota_reale", F.col('Postalizzazioni') * F.col('PROP_POP_CAP_NAZ'))\
                                                     .withColumn("floor_val", F.floor(F.col("quota_reale")))\
                                                     .withColumn("resto", F.col("quota_reale") - F.col("floor_val"))\
                                                     .select('PRODUCT_TYPE','PA_ID','DELIVERY_DATE','Postalizzazioni','CAP','COD_SIGLA_PROVINCIA','quota_reale','floor_val','resto')
    
    df_postalizzazioni_cap_naz.show()
    
    # Calcolo il numero di postalizzazioni per CAP e ridistribuisco i resti
    column_list = ['PRODUCT_TYPE','PA_ID','DELIVERY_DATE']
    win_spec = Window.partitionBy([F.col(x) for x in column_list])
    
    df_postalizzazioni_mancanti_naz = df_postalizzazioni_cap_naz.withColumn("somma_floor", F.sum("floor_val").over(win_spec))\
                                                                .withColumn("totale_gruppo", F.max('Postalizzazioni').over(win_spec))\
                                                                .withColumn("manca", F.col("totale_gruppo") - F.col("somma_floor"))
    
    w_group_desc_resto = Window.partitionBy([F.col(x) for x in column_list]).orderBy(F.col("resto").desc())
    
    df_ranking_naz = df_postalizzazioni_mancanti_naz.withColumn("rank_resti", F.row_number().over(w_group_desc_resto))
    
    df_postalizzazioni_cap_final_naz = df_ranking_naz.withColumn(
            "Postalizzazioni_cap",
            F.col("floor_val") +
            F.when(F.col("rank_resti") <= F.col("manca"), 1).otherwise(0)
        )
    
    
    print('Split postalizzazioni per CAP sui territori regionali')
    # Lavorazione per suddivisione geografica regionale
    w_reg = Window.partitionBy('REGIONE')
    df_cap_prov_prop_reg=df_cap_prov.withColumn('PROP_POP_CAP_REG',F.col('POP_CAP')/F.sum(F.col('POP_CAP')).over(w_reg))
    
    # Suddivisione delle postalizzazioni settimanali per CAP
    lista_regioni=[reg['REGIONE'] for reg in df_cap_prov.select(F.col('REGIONE')).distinct().collect()]
    df_postalizzazioni_reg=df_postalizzazioni.filter(F.col('SUDDIVISIONE_GEOGRAFICA').isin(lista_regioni))
    
    df_postalizzazioni_cap_reg=df_postalizzazioni_reg.join(df_cap_prov_prop_reg,df_postalizzazioni.SUDDIVISIONE_GEOGRAFICA==df_cap_prov_prop_reg.REGIONE,how='left')\
                                                     .withColumn("quota_reale", F.col('Postalizzazioni') * F.col('PROP_POP_CAP_REG'))\
                                                     .withColumn("floor_val", F.floor(F.col("quota_reale")))\
                                                     .withColumn("resto", F.col("quota_reale") - F.col("floor_val"))\
                                                     .select('PRODUCT_TYPE','PA_ID','DELIVERY_DATE','Postalizzazioni','REGIONE','CAP','quota_reale','floor_val','resto')
    
    # Calcolo il numero di postalizzazioni per CAP e ridistribuisco i resti
    column_list = ['PRODUCT_TYPE','PA_ID','DELIVERY_DATE','REGIONE']
    win_spec = Window.partitionBy([F.col(x) for x in column_list])
    
    df_postalizzazioni_mancanti_reg = df_postalizzazioni_cap_reg.withColumn("somma_floor", F.sum("floor_val").over(win_spec))\
                                                                .withColumn("totale_gruppo", F.max('Postalizzazioni').over(win_spec))\
                                                                .withColumn("manca", F.col("totale_gruppo") - F.col("somma_floor"))
    
    w_group_desc_resto = Window.partitionBy([F.col(x) for x in column_list]).orderBy(F.col("resto").desc())
    
    df_ranking_reg = df_postalizzazioni_mancanti_reg.withColumn("rank_resti", F.row_number().over(w_group_desc_resto))
    
    df_postalizzazioni_cap_final_reg = df_ranking_reg.withColumn(
            "Postalizzazioni_cap",
            F.col("floor_val") +
            F.when(F.col("rank_resti") <= F.col("manca"), 1).otherwise(0)
        )
    
    
    print('Split postalizzazioni per CAP sui territori provinciali')
    # Lavorazione per suddivisione geografica provinciale
    # Suddivisione delle postalizzazioni settimanali per CAP
    lista_province=[prov['COD_SIGLA_PROVINCIA'] for prov in df_cap_prov.select(F.col('COD_SIGLA_PROVINCIA')).distinct().collect()]
    df_postalizzazioni_prov=df_postalizzazioni.filter(F.col('SUDDIVISIONE_GEOGRAFICA').isin(lista_province))
    
    df_postalizzazioni_cap_prov=df_postalizzazioni_prov.join(df_cap_prov,df_postalizzazioni.SUDDIVISIONE_GEOGRAFICA==df_cap_prov.COD_SIGLA_PROVINCIA,how='left')\
                                                       .withColumn("quota_reale", F.col('Postalizzazioni') * F.col('PERCENTUALE_POP_CAP'))\
                                                       .withColumn("floor_val", F.floor(F.col("quota_reale")))\
                                                       .withColumn("resto", F.col("quota_reale") - F.col("floor_val"))\
                                                       .select('PRODUCT_TYPE','PA_ID','DELIVERY_DATE','Postalizzazioni','COD_SIGLA_PROVINCIA','CAP','quota_reale','floor_val','resto')
                                                       
    # Calcolo il numero di postalizzazioni per CAP e ridistribuisco i resti
    column_list = ['PRODUCT_TYPE','PA_ID','DELIVERY_DATE','COD_SIGLA_PROVINCIA']
    win_spec = Window.partitionBy([F.col(x) for x in column_list])
    
    df_postalizzazioni_mancanti_prov = df_postalizzazioni_cap_prov.withColumn("somma_floor", F.sum("floor_val").over(win_spec))\
                                                                  .withColumn("totale_gruppo", F.max('Postalizzazioni').over(win_spec))\
                                                                  .withColumn("manca", F.col("totale_gruppo") - F.col("somma_floor"))
    
    w_group_desc_resto = Window.partitionBy([F.col(x) for x in column_list]).orderBy(F.col("resto").desc())
    
    df_ranking_prov = df_postalizzazioni_mancanti_prov.withColumn("rank_resti", F.row_number().over(w_group_desc_resto))
    
    df_postalizzazioni_cap_final_prov = df_ranking_prov.withColumn(
            "Postalizzazioni_cap",
            F.col("floor_val") +
            F.when(F.col("rank_resti") <= F.col("manca"), 1).otherwise(0)
        )
    
    
    print('Unione datasets')
    # Unione dei dati delle 3 fasi
    df_postalizzazioni_cap_final_naz_v1=df_postalizzazioni_cap_final_naz.join(df_cap_prov,on=['CAP','COD_SIGLA_PROVINCIA'],how='left')\
                                                                        .select('CAP','PRODUCT_TYPE','PA_ID',df_postalizzazioni_cap_final_naz['COD_SIGLA_PROVINCIA'],'DELIVERY_DATE','Postalizzazioni_cap')
                                                                        
    df_postalizzazioni_cap_final_reg_v1=df_postalizzazioni_cap_final_reg.join(df_cap_prov,on=['CAP','REGIONE'],how='left')\
                                                                        .select('CAP','PRODUCT_TYPE','PA_ID','COD_SIGLA_PROVINCIA','DELIVERY_DATE','Postalizzazioni_cap')
                                                                        
    df_postalizzazioni_cap_final_prov_v1=df_postalizzazioni_cap_final_prov.select('CAP','PRODUCT_TYPE','PA_ID','COD_SIGLA_PROVINCIA','DELIVERY_DATE','Postalizzazioni_cap')
                                                                        
    df_postalizzazioni_cap_final_stg = df_postalizzazioni_cap_final_naz_v1.union(df_postalizzazioni_cap_final_reg_v1)\
                                                                          .union(df_postalizzazioni_cap_final_prov_v1)
                                                                      
    df_postalizzazioni_cap_final = df_postalizzazioni_cap_final_stg.withColumn('Postalizzazioni_cap',(F.col('Postalizzazioni_cap')).cast(T.IntegerType()))
    
    df_postalizzazioni_cap_final.show()
    
    
    print('Esplosione del dataframe')
    # 'Esplosione' del dataframe in più righe quante sono le postalizzazioni
    df_postalizzazioni_exploded=df_postalizzazioni_cap_final.withColumn('array_rep',F.array_repeat(F.lit(None),df_postalizzazioni_cap_final['Postalizzazioni_cap']))\
                                                            .withColumn('array_rep_v1',F.explode('array_rep'))
    
    
    df_postalizzazioni_final=df_postalizzazioni_exploded.withColumn('iun',F.concat((F.monotonically_increasing_id()+1000000).cast(T.StringType()),F.lit('_MOCK')))\
                                                        .withColumn('prepareRequestDate',F.to_timestamp(F.col('DELIVERY_DATE'),"yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'"))\
                                                        .withColumn('workflowStep',F.lit('EVALUATE_SENDER_LIMIT'))\
                                                        .withColumn('NotificationSentAt',F.to_timestamp(F.col('DELIVERY_DATE'),"yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'"))\
                                                        .withColumn('RequestID',F.concat((F.monotonically_increasing_id()+1000000).cast(T.StringType()),F.lit('_MOCK')))\
                                                        .withColumn('senderPaId',F.concat(F.col('PA_ID'),F.lit('_MOCK')))\
                                                        .withColumn('attempt',F.lit(0))\
                                                        .withColumnRenamed('COD_SIGLA_PROVINCIA','province')\
                                                        .withColumnRenamed('PRODUCT_TYPE','productType')\
                                                        .withColumnRenamed('CAP','cap')\
                                                        .select('RequestID','notificationSentAt','prepareRequestDate','productType','senderPaId','province','cap','attempt','iun')
    
    print('Export in S3')
    # Export in csv a lotti di 10.000 righe
    
    #calcolo data parametro per import
    if lista_lunedi[0]!=mese:
        del lista_lunedi[0]
    
    lunedì_mese_successivo = lista_lunedi[-1] + timedelta(days=7)
    lista_lunedi.append(lunedì_mese_successivo)  
    
    print('lista_lunedi:',lista_lunedi)
    
    
    output_prefix = None
    s3_client = boto3.client('s3')
    target_date = date.today()
    
    for _ in range(120):  # limite di sicurezza a 30 gg
        input_prefix = target_date.strftime("%Y/%m/%d/")
        response = s3_client.list_objects_v2(
            Bucket=s3_bucket,
            Prefix='input/'+input_prefix+args['mese_simulazione'][:7]+'/',
            MaxKeys=1
        )
        # se la cartella esiste, esco dal ciclo
        if 'Contents' in response:
            output_prefix = 'input/'+input_prefix+args['mese_simulazione'][:7]+'/'
            break
        # altrimenti vado al giorno precedente
        target_date -= timedelta(days=1)
    
    
    anno_str = args['mese_simulazione'][:4]
    mese_str = args['mese_simulazione'][5:7]
    
    
    if output_prefix == None:
        # se non viene trovata alcuna cartella corrispondente
        raise Exception("Nessuna folder input/yyyy/MM/dd_di_estrazione/yyyy_MM_simulazione su S3 creata negli ultimi 30 gg")
        
    else:
        path = "s3://" + s3_bucket+"/" + output_prefix + "postalizzazioni_mock/" + "ID_" + str(id_simulazione)
                                                                  
        
        #suddivisione dataset in settimane
        for lunedi in lista_lunedi:
            if lunedi == lista_lunedi[0]:
                df_split = df_postalizzazioni_final.filter(F.col('prepareRequestDate') < lunedi )
            else:
                df_split = df_postalizzazioni_final.filter((F.col('prepareRequestDate') < lunedi) & (F.col('prepareRequestDate') >= (lunedi + timedelta(days=-7))))
            df_split.show(1)
            num_rows=df_split.count()
            part=math.ceil(num_rows/max_rows)
            if part>0:
                df_split.repartition(part).write.mode('overwrite').option('header',True).option('sep',';').option('quoteAll','true').format('csv').save(path + "/" + str(lunedi))

        

# da lasciare come ultimo comando per indicare che il job ha terminato con SUCCESS la sua esecuzione
job.commit()
 