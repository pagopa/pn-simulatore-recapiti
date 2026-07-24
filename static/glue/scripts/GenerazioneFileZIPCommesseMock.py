import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','mese_simulazione','id_simulazione_manuale','s3_bucket','secretsManager_SecretId','jdbc_connection'])
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])
# mese_simulazione = '2025-04-06'

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
import json
import zipfile
import os
from botocore.config import Config
import requests
import io

# recupero parametri d'ambiente del job
s3_bucket = args['s3_bucket']
secretsManager_SecretId = args['secretsManager_SecretId']
jdbc_connection = args['jdbc_connection']
mese_simulazione = args['mese_simulazione']
id_simulazione = int(args['id_simulazione_manuale']) # tipo: stringa
max_rows = 10000

print('Import della tabella con i dati demografici di CAP e province')
######################################
# Import della tabella con i dati demografici di CAP e province

#recupero credenziali db da secretsmanager
client = boto3.client("secretsmanager")
response = client.get_secret_value(SecretId=secretsManager_SecretId)
response_SecretString = json.loads(response['SecretString'])


####################
print('Lettura CAP_PROV_REG')

db_table = 'public."CAP_PROV_REG"'

df_cap_prov_reg = spark.read \
    .format("jdbc") \
    .option("url", jdbc_connection) \
    .option("dbtable", db_table) \
    .option("user", response_SecretString['username']) \
    .option("password", response_SecretString['password']) \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_cap_prov_reg = df_cap_prov_reg.withColumn('REGIONE',F.when(F.col('REGIONE')=="Valle d'Aosta","Valle d'Aosta/Vallée d'Aoste").otherwise(F.col('REGIONE')))

###########
print('Lettura SENDER_LIMIT')

db_table = 'public."SENDER_LIMIT"'

df_senderlim = spark.read \
    .format("jdbc") \
    .option("url", jdbc_connection) \
    .option("dbtable", db_table) \
    .option("user", response_SecretString['username']) \
    .option("password", response_SecretString['password']) \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_senderlim = df_senderlim.drop('ID')\
                           .filter(F.col('DELIVERY_DATE')==mese_simulazione)


###########
print('Lettura SENDER_LIMIT_MOCK')

db_table = 'public."SENDER_LIMIT_MOCK"'

df_senderlim_mock = spark.read \
    .format("jdbc") \
    .option("url", jdbc_connection) \
    .option("dbtable", db_table) \
    .option("user", response_SecretString['username']) \
    .option("password", response_SecretString['password']) \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_senderlim_mock.show()

df_senderlim_mock = df_senderlim_mock.drop('ID')\
                                     .withColumn('ATTEMPT',F.lit(0))\
                                     .filter(F.col('SIMULAZIONE_ID')==id_simulazione)

###########
print('Lettura SIMULAZIONE')

db_table = 'public."SIMULAZIONE"'

df_simulazione = spark.read \
    .format("jdbc") \
    .option("url", jdbc_connection) \
    .option("dbtable", db_table) \
    .option("user", response_SecretString['username']) \
    .option("password", response_SecretString['password']) \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_simulazione.show()

df_simulazione = df_simulazione.filter(F.col('ID')==F.lit(id_simulazione))

# Decisione dell'azione in base alla pianificazione
pianificazione_postalizzazioni = (df_simulazione.select('PIANIFICAZIONE_POSTALIZZAZIONI').collect()[0])['PIANIFICAZIONE_POSTALIZZAZIONI']

# Individuazione del path di scrittura temporanea
# tmp_dir = '/tmp'
mese_simulazione_path = mese_simulazione[:4] + '_' + mese_simulazione[5:7]
file_zip = "Commesse_enti_"+mese_simulazione_path+"_ID"+str(id_simulazione)+".zip"
# tmp_path = tmp_dir + "/" + file_zip

# # Eliminazione del file zip nel caso si trovi già all'interno della cartella
# tmp_list = os.listdir(tmp_dir)
# for el in tmp_list:
#     if el==file_zip:
#         os.remove(tmp_path)
#         print('Pulizia della cartella temporanea effettuata')  

zip_buffer = io.BytesIO()

if pianificazione_postalizzazioni == 'Utilizza le commesse di default e le commesse di mock':


    # Lavorazione su SENDER_LIMIT
    print('Lavorazione su SENDER_LIMIT')

    # Aggiunta della regione alla tabella
    df_cap_prov_reg_distinct = df_cap_prov_reg.select('COD_SIGLA_PROVINCIA','REGIONE').distinct()

    df_senderlim_reg = df_senderlim.join(df_cap_prov_reg_distinct, df_senderlim.PROVINCE == df_cap_prov_reg_distinct.COD_SIGLA_PROVINCIA, 'left')\
                                   .select(df_senderlim['*'],df_cap_prov_reg_distinct['REGIONE'])

    # Aggregazione per regione
    df_senderlim_reg_grouped = df_senderlim_reg.groupBy('PA_ID','DELIVERY_DATE','PRODUCT_TYPE','REGIONE')\
                                               .agg(F.sum('MONTHLY_ESTIMATE').alias('MONTHLY_ESTIMATE'), F.max('LAST_UPDATE_TIMESTAMP').alias('LAST_UPDATE_TIMESTAMP'))

    print('Scrittura file')
    with zipfile.ZipFile(zip_buffer, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
        
        for ente in df_senderlim_reg_grouped.select("PA_ID").distinct().collect():
            
            ente = ente.asDict()['PA_ID']

            # Se l'ente è un extra la commessa non deve essere creata
            if 'FUORI_COMMESSA' not in ente.upper(): 
                print('Elaborazione ente: ',ente)
                
                diz_ente_json = {}
                df_senderlim_ente = df_senderlim_reg_grouped.filter(F.col('PA_ID')==ente)
            
                # Riempimento dei campi a livello di ente
                diz_ente_json['idEnte'] = ente
                diz_ente_json['contractId'] = ente
                df_senderlim_ente_modified = df_senderlim_ente.withColumn('DELIVERY_DATE',F.date_format('DELIVERY_DATE',"M-yyyy"))\
                                                              .withColumn('LAST_UPDATE_TIMESTAMP',F.date_format('LAST_UPDATE_TIMESTAMP',"yyyy-MM-dd'T'HH:mm:ss"))  
                diz_ente_json['periodo_riferimento'] = str(df_senderlim_ente_modified.select("DELIVERY_DATE").collect()[0].asDict()['DELIVERY_DATE'])
                diz_ente_json['last_update'] = str(df_senderlim_ente_modified.select("LAST_UPDATE_TIMESTAMP").collect()[0].asDict()['LAST_UPDATE_TIMESTAMP'])

                # Riempimento dei campi a livello di prodotto
                list_prod_json = []
                
                for prodotto in df_senderlim_ente.select("PRODUCT_TYPE").distinct().collect():
            
                    prodotto = prodotto.asDict()['PRODUCT_TYPE']
                    df_senderlim_prod = df_senderlim_ente.filter(F.col('PRODUCT_TYPE')==prodotto)
            
                    diz_prod_json = {}
                    diz_prod_json['id'] = prodotto
                    diz_prod_json['nome'] = prodotto
            
                    sum_monthly_estimate_prod = 0
                    for row in df_senderlim_prod.select('MONTHLY_ESTIMATE').collect():
                        sum_monthly_estimate_prod += row['MONTHLY_ESTIMATE']
                    diz_prod_json['valore_totale'] = sum_monthly_estimate_prod
            
                    # Riempimento dei campi per nazione
                    diz_prod_json['varianti'] = []
                    
                    diz_naz_json_nz = {}
                    diz_naz_json_nz['codice'] = 'NZ'
                    diz_naz_json_nz['nome'] = 'NZ'
                    diz_naz_json_nz['valore_totale'] = sum_monthly_estimate_prod
                    diz_naz_json_nz['distribuzione'] = {'regionale':[]}
            
                    # Riempimento campi per regione
                    list_reg_json = []
            
                    for regione in df_senderlim_prod.select('REGIONE').collect():
            
                        regione = regione.asDict()['REGIONE']
                        diz_reg_json = {}
                        diz_reg_json['regione'] = regione
                        diz_reg_json['province'] = None
            
                        sum_monthly_estimate_reg = 0
                        for row in df_senderlim_prod.select('MONTHLY_ESTIMATE').filter(F.col('REGIONE')==regione).collect():
                            sum_monthly_estimate_reg += row['MONTHLY_ESTIMATE']
                        diz_reg_json['valore'] = sum_monthly_estimate_reg
            
                        list_reg_json.append(diz_reg_json)
                    
                    diz_naz_json_nz['distribuzione'] = {'regionale': list_reg_json}
            
                    diz_prod_json['varianti'].append(diz_naz_json_nz)
                    
                    if prodotto == 'AR':
                        diz_naz_json_int = {}
                        diz_naz_json_int['codice'] = 'INT'
                        diz_naz_json_int['nome'] = 'INT'
                        diz_naz_json_int['valore_totale'] = 0
                        diz_naz_json_int['distribuzione'] = None
                        
                        diz_prod_json['varianti'].append(diz_naz_json_int)
            
                    list_prod_json.append(diz_prod_json)
            
                # Aggiunta del prodotto digitale
                diz_digitale = {
                        "id": "digitale",
                        "nome": "digitale",
                        "valore_totale": 0,
                        "varianti": [
                            {
                                "codice": "PEC",
                                "nome": "PEC",
                                "valore_totale": 0,
                                "distribuzione": None
                            }
                        ]
                    }
                
                list_prod_json.append(diz_digitale)
            
                diz_ente_json['prodotti'] = list_prod_json
        
                # Scrittura del Json dell'ente all'interno del file zip
                str_ente_json = json.dumps(diz_ente_json, indent=4, ensure_ascii=False)
                zipf.writestr(ente+".json", str_ente_json)



    # Lavorazione su SENDER_LIMIT_MOCK
    print('Lavorazione su SENDER_LIMIT_MOCK')

    # Aggiunta della regione alla tabella
    df_cap_prov_reg_distinct = df_cap_prov_reg.select('COD_SIGLA_PROVINCIA','REGIONE').distinct()

    df_senderlim_mock_reg_stg = df_senderlim_mock.join(df_cap_prov_reg_distinct, df_senderlim_mock.SUDDIVISIONE_GEOGRAFICA == df_cap_prov_reg_distinct.COD_SIGLA_PROVINCIA, 'left')\
                                   .select(df_senderlim_mock['*'],df_cap_prov_reg_distinct['REGIONE'])

    # Aggiusto per i valori che hanno già regioni
    df_senderlim_mock_reg = df_senderlim_mock_reg_stg.withColumn('REGIONE',F.when((F.col('REGIONE').isNull()) & (F.col('SUDDIVISIONE_GEOGRAFICA')!='Italia'), F.col('SUDDIVISIONE_GEOGRAFICA'))\
                                                            .otherwise(F.col('REGIONE')))

    # Aggiunta alla CAP_PROV_REG della proporzione nazionale per CAP
    pop_cap_tot = df_cap_prov_reg.select(F.sum(F.col('POP_CAP'))).collect()[0].asDict()['sum(POP_CAP)']
    df_cap_prov_prop_naz=df_cap_prov_reg.withColumn('PROP_POP_CAP_NAZ',F.col('POP_CAP')/pop_cap_tot)\
                                            .withColumn('SUDDIVISIONE_NAZIONALE',F.lit('Italia'))


    # Scrittura file
    print('Scrittura file')
    with zipfile.ZipFile(zip_buffer, "a", compression=zipfile.ZIP_DEFLATED) as zipf:
        
        for ente in df_senderlim_mock_reg.select("PA_ID").distinct().collect():
            
            ente = ente.asDict()['PA_ID']

            # Se l'ente è un extra la commessa non deve essere creata
            if 'FUORI_COMMESSA' not in ente.upper(): 
                print('Elaborazione ente: ',ente)
            
                # Lavorazione a parte per le commesse a livello nazionale
                df_senderlim_mock_ente = df_senderlim_mock_reg.filter(F.col('PA_ID')==ente)
                
                # Suddivisione delle postalizzazioni per CAP
                df_senderlim_mock_naz=df_senderlim_mock_ente.filter(F.col('SUDDIVISIONE_GEOGRAFICA')=='Italia')\
                                                            .withColumnRenamed('REGIONE','REGIONE_SENDER')
                
                df_postalizzazioni_cap_naz=df_senderlim_mock_naz.join(df_cap_prov_prop_naz,df_senderlim_mock_naz.SUDDIVISIONE_GEOGRAFICA==df_cap_prov_prop_naz.SUDDIVISIONE_NAZIONALE,how='left')\
                                                                 .withColumn("quota_reale", F.col('MONTHLY_ESTIMATE') * F.col('PROP_POP_CAP_NAZ'))\
                                                                 .withColumn("floor_val", F.floor(F.col("quota_reale")))\
                                                                 .withColumn("resto", F.col("quota_reale") - F.col("floor_val"))\
                                                                 .select('SIMULAZIONE_ID','ATTEMPT','LAST_UPDATE_TIMESTAMP','PRODUCT_TYPE','PA_ID','DELIVERY_DATE','MONTHLY_ESTIMATE','CAP','COD_SIGLA_PROVINCIA','REGIONE','quota_reale','floor_val','resto')
                
                # Calcolo il numero di postalizzazioni per CAP e ridistribuisco i resti
                column_list = ['PRODUCT_TYPE','DELIVERY_DATE']
                win_spec = Window.partitionBy([F.col(x) for x in column_list])
                
                df_postalizzazioni_mancanti_naz = df_postalizzazioni_cap_naz.withColumn("somma_floor", F.sum("floor_val").over(win_spec))\
                                                                            .withColumn("totale_gruppo", F.max('MONTHLY_ESTIMATE').over(win_spec))\
                                                                            .withColumn("manca", F.col("totale_gruppo") - F.col("somma_floor"))
                
                w_group_desc_resto = Window.partitionBy([F.col(x) for x in column_list]).orderBy(F.col("resto").desc())
                
                df_ranking_naz = df_postalizzazioni_mancanti_naz.withColumn("rank_resti", F.row_number().over(w_group_desc_resto))
                
                df_postalizzazioni_cap_final_naz = df_ranking_naz.withColumn(
                        "Postalizzazioni_cap",
                        F.col("floor_val") +
                        F.when(F.col("rank_resti") <= F.col("manca"), 1).otherwise(0)
                    )\
                    .drop('MONTHLY_ESTIMATE')
            
                # Unione dei dati derivati dalle commesse nazionali e dalle altre
                df_senderlim_mock_naz = df_postalizzazioni_cap_final_naz.withColumnRenamed('Postalizzazioni_cap','MONTHLY_ESTIMATE')\
                                                .withColumnRenamed('COD_SIGLA_PROVINCIA','SUDDIVISIONE_GEOGRAFICA')\
                                                .select('SIMULAZIONE_ID','DELIVERY_DATE','PA_ID','MONTHLY_ESTIMATE','PRODUCT_TYPE','SUDDIVISIONE_GEOGRAFICA','ATTEMPT','LAST_UPDATE_TIMESTAMP','REGIONE')
                
                df_senderlim_mock_subnaz=df_senderlim_mock_ente.filter(F.col('SUDDIVISIONE_GEOGRAFICA')!='Italia')\
                                                               .select('SIMULAZIONE_ID','DELIVERY_DATE','PA_ID','MONTHLY_ESTIMATE','PRODUCT_TYPE','SUDDIVISIONE_GEOGRAFICA','ATTEMPT','LAST_UPDATE_TIMESTAMP','REGIONE')
                
                df_senderlim_mock_tot = df_senderlim_mock_naz.union(df_senderlim_mock_subnaz)

                # Aggregazione per regione
                df_senderlim_mock_tot_grouped = df_senderlim_mock_tot.groupBy('PA_ID','DELIVERY_DATE','SIMULAZIONE_ID','PRODUCT_TYPE','REGIONE')\
                                                                     .agg(F.sum('MONTHLY_ESTIMATE').alias('MONTHLY_ESTIMATE'), F.max('LAST_UPDATE_TIMESTAMP').alias('LAST_UPDATE_TIMESTAMP'))
                
                # CREAZIONE JSON
                
                diz_ente_json = {}
                
                # Riempimento dei campi a livello di ente
                diz_ente_json['idEnte'] = ente
                diz_ente_json['contractId'] = ente
                df_senderlim_mock_tot_grouped_modified = df_senderlim_mock_tot_grouped.withColumn('DELIVERY_DATE',F.date_format('DELIVERY_DATE',"M-yyyy"))\
                                                            .withColumn('LAST_UPDATE_TIMESTAMP',F.date_format('LAST_UPDATE_TIMESTAMP',"yyyy-MM-dd'T'HH:mm:ss"))  
                diz_ente_json['periodo_riferimento'] = str(df_senderlim_mock_tot_grouped_modified.select("DELIVERY_DATE").collect()[0].asDict()['DELIVERY_DATE'])
                diz_ente_json['last_update'] = str(df_senderlim_mock_tot_grouped_modified.select("LAST_UPDATE_TIMESTAMP").collect()[0].asDict()['LAST_UPDATE_TIMESTAMP'])
            
                # Riempimento dei campi a livello di prodotto
                list_prod_json = []
                
                for prodotto in df_senderlim_mock_tot_grouped.select("PRODUCT_TYPE").distinct().collect():
            
                    prodotto = prodotto.asDict()['PRODUCT_TYPE']
                    diz_prod_json = {}
                    diz_prod_json['id'] = prodotto
                    diz_prod_json['nome'] = prodotto
            
                    df_senderlim_mock_prod = df_senderlim_mock_tot_grouped.filter(F.col('PRODUCT_TYPE')==prodotto)
            
                    sum_monthly_estimate_prod = 0
                    for row in df_senderlim_mock_prod.select('MONTHLY_ESTIMATE').collect():
                        sum_monthly_estimate_prod += row['MONTHLY_ESTIMATE']
                    diz_prod_json['valore_totale'] = sum_monthly_estimate_prod
            
                    # Riempimento dei campi per nazione
                    diz_prod_json['varianti'] = []
                    
                    diz_naz_json_nz = {}
                    diz_naz_json_nz['codice'] = 'NZ'
                    diz_naz_json_nz['nome'] = 'NZ'
                    diz_naz_json_nz['valore_totale'] = sum_monthly_estimate_prod
                    diz_naz_json_nz['distribuzione'] = {'regionale':[]}
            
                    # Riempimento campi per regione
                    list_reg_json = []
            
                    for regione in df_senderlim_mock_prod.select('REGIONE').collect():
            
                        regione = regione.asDict()['REGIONE']
                        diz_reg_json = {}
                        diz_reg_json['regione'] = regione
                        diz_reg_json['province'] = None
            
                        sum_monthly_estimate_reg = 0
                        for row in df_senderlim_mock_prod.select('MONTHLY_ESTIMATE').filter(F.col('REGIONE')==regione).collect():
                            sum_monthly_estimate_reg += row['MONTHLY_ESTIMATE']
                        diz_reg_json['valore'] = sum_monthly_estimate_reg
            
                        list_reg_json.append(diz_reg_json)
            
                    diz_naz_json_nz['distribuzione'] = {'regionale': list_reg_json}
            
                    diz_prod_json['varianti'].append(diz_naz_json_nz)
                    
                    if prodotto == 'AR':
                        diz_naz_json_int = {}
                        diz_naz_json_int['codice'] = 'INT'
                        diz_naz_json_int['nome'] = 'INT'
                        diz_naz_json_int['valore_totale'] = 0
                        diz_naz_json_int['distribuzione'] = None
                        
                        diz_prod_json['varianti'].append(diz_naz_json_int)
            
                    list_prod_json.append(diz_prod_json)
            
                # Aggiunta del prodotto digitale
                diz_digitale = {
                        "id": "digitale",
                        "nome": "digitale",
                        "valore_totale": 0,
                        "varianti": [
                            {
                                "codice": "PEC",
                                "nome": "PEC",
                                "valore_totale": 0,
                                "distribuzione": None
                            }
                        ]
                    }
                
                list_prod_json.append(diz_digitale)
            
                diz_ente_json['prodotti'] = list_prod_json
                
                
                # Scrittura del Json dell'ente all'interno del file zip
                str_ente_json = json.dumps(diz_ente_json, indent=4, ensure_ascii=False)
                zipf.writestr(ente+".json", str_ente_json)
        


if pianificazione_postalizzazioni == 'Utilizza solo le commesse di mock':
    
    # Lavorazione su SENDER_LIMIT_MOCK
    print('Lavorazione su SENDER_LIMIT_MOCK')

    # Aggiunta della regione alla tabella
    df_cap_prov_reg_distinct = df_cap_prov_reg.select('COD_SIGLA_PROVINCIA','REGIONE').distinct()

    df_senderlim_mock_reg_stg = df_senderlim_mock.join(df_cap_prov_reg_distinct, df_senderlim_mock.SUDDIVISIONE_GEOGRAFICA == df_cap_prov_reg_distinct.COD_SIGLA_PROVINCIA, 'left')\
                                   .select(df_senderlim_mock['*'],df_cap_prov_reg_distinct['REGIONE'])

    # Aggiusto per i valori che hanno già regioni
    df_senderlim_mock_reg = df_senderlim_mock_reg_stg.withColumn('REGIONE',F.when((F.col('REGIONE').isNull()) & (F.col('SUDDIVISIONE_GEOGRAFICA')!='Italia'), F.col('SUDDIVISIONE_GEOGRAFICA'))\
                                                            .otherwise(F.col('REGIONE')))

    # Aggiunta alla CAP_PROV_REG della proporzione nazionale per CAP
    pop_cap_tot = df_cap_prov_reg.select(F.sum(F.col('POP_CAP'))).collect()[0].asDict()['sum(POP_CAP)']
    df_cap_prov_prop_naz=df_cap_prov_reg.withColumn('PROP_POP_CAP_NAZ',F.col('POP_CAP')/pop_cap_tot)\
                                            .withColumn('SUDDIVISIONE_NAZIONALE',F.lit('Italia'))


    # Scrittura file
    print('Scrittura file')
    with zipfile.ZipFile(zip_buffer, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
        
        for ente in df_senderlim_mock_reg.select("PA_ID").distinct().collect():
            
            ente = ente.asDict()['PA_ID']

            # Se l'ente è un extra la commessa non deve essere creata
            if 'FUORI_COMMESSA' not in ente.upper(): 
                print('Elaborazione ente: ',ente)
            
                # Lavorazione a parte per le commesse a livello nazionale
                df_senderlim_mock_ente = df_senderlim_mock_reg.filter(F.col('PA_ID')==ente)
                
                # Suddivisione delle postalizzazioni per CAP
                df_senderlim_mock_naz=df_senderlim_mock_ente.filter(F.col('SUDDIVISIONE_GEOGRAFICA')=='Italia')\
                                                            .withColumnRenamed('REGIONE','REGIONE_SENDER')
                
                df_postalizzazioni_cap_naz=df_senderlim_mock_naz.join(df_cap_prov_prop_naz,df_senderlim_mock_naz.SUDDIVISIONE_GEOGRAFICA==df_cap_prov_prop_naz.SUDDIVISIONE_NAZIONALE,how='left')\
                                                                 .withColumn("quota_reale", F.col('MONTHLY_ESTIMATE') * F.col('PROP_POP_CAP_NAZ'))\
                                                                 .withColumn("floor_val", F.floor(F.col("quota_reale")))\
                                                                 .withColumn("resto", F.col("quota_reale") - F.col("floor_val"))\
                                                                 .select('SIMULAZIONE_ID','ATTEMPT','LAST_UPDATE_TIMESTAMP','PRODUCT_TYPE','PA_ID','DELIVERY_DATE','MONTHLY_ESTIMATE','CAP','COD_SIGLA_PROVINCIA','REGIONE','quota_reale','floor_val','resto')
                
                # Calcolo il numero di postalizzazioni per CAP e ridistribuisco i resti
                column_list = ['PRODUCT_TYPE','DELIVERY_DATE']
                win_spec = Window.partitionBy([F.col(x) for x in column_list])
                
                df_postalizzazioni_mancanti_naz = df_postalizzazioni_cap_naz.withColumn("somma_floor", F.sum("floor_val").over(win_spec))\
                                                                            .withColumn("totale_gruppo", F.max('MONTHLY_ESTIMATE').over(win_spec))\
                                                                            .withColumn("manca", F.col("totale_gruppo") - F.col("somma_floor"))
                
                w_group_desc_resto = Window.partitionBy([F.col(x) for x in column_list]).orderBy(F.col("resto").desc())
                
                df_ranking_naz = df_postalizzazioni_mancanti_naz.withColumn("rank_resti", F.row_number().over(w_group_desc_resto))
                
                df_postalizzazioni_cap_final_naz = df_ranking_naz.withColumn(
                        "Postalizzazioni_cap",
                        F.col("floor_val") +
                        F.when(F.col("rank_resti") <= F.col("manca"), 1).otherwise(0)
                    )\
                    .drop('MONTHLY_ESTIMATE')
            
                # Unione dei dati derivati dalle commesse nazionali e dalle altre
                df_senderlim_mock_naz = df_postalizzazioni_cap_final_naz.withColumnRenamed('Postalizzazioni_cap','MONTHLY_ESTIMATE')\
                                                .withColumnRenamed('COD_SIGLA_PROVINCIA','SUDDIVISIONE_GEOGRAFICA')\
                                                .select('SIMULAZIONE_ID','DELIVERY_DATE','PA_ID','MONTHLY_ESTIMATE','PRODUCT_TYPE','SUDDIVISIONE_GEOGRAFICA','ATTEMPT','LAST_UPDATE_TIMESTAMP','REGIONE')
                
                df_senderlim_mock_subnaz=df_senderlim_mock_ente.filter(F.col('SUDDIVISIONE_GEOGRAFICA')!='Italia')\
                                                               .select('SIMULAZIONE_ID','DELIVERY_DATE','PA_ID','MONTHLY_ESTIMATE','PRODUCT_TYPE','SUDDIVISIONE_GEOGRAFICA','ATTEMPT','LAST_UPDATE_TIMESTAMP','REGIONE')
                
                df_senderlim_mock_tot = df_senderlim_mock_naz.union(df_senderlim_mock_subnaz)

                # Aggregazione per regione
                df_senderlim_mock_tot_grouped = df_senderlim_mock_tot.groupBy('PA_ID','DELIVERY_DATE','SIMULAZIONE_ID','PRODUCT_TYPE','REGIONE')\
                                                                     .agg(F.sum('MONTHLY_ESTIMATE').alias('MONTHLY_ESTIMATE'), F.max('LAST_UPDATE_TIMESTAMP').alias('LAST_UPDATE_TIMESTAMP'))
                
                # CREAZIONE JSON
                
                diz_ente_json = {}
                
                # Riempimento dei campi a livello di ente
                diz_ente_json['idEnte'] = ente
                diz_ente_json['contractId'] = ente
                df_senderlim_mock_tot_grouped_modified = df_senderlim_mock_tot_grouped.withColumn('DELIVERY_DATE',F.date_format('DELIVERY_DATE',"M-yyyy"))\
                                                              .withColumn('LAST_UPDATE_TIMESTAMP',F.date_format('LAST_UPDATE_TIMESTAMP',"yyyy-MM-dd'T'HH:mm:ss"))  
                diz_ente_json['periodo_riferimento'] = str(df_senderlim_mock_tot_grouped_modified.select("DELIVERY_DATE").collect()[0].asDict()['DELIVERY_DATE'])
                diz_ente_json['last_update'] = str(df_senderlim_mock_tot_grouped_modified.select("LAST_UPDATE_TIMESTAMP").collect()[0].asDict()['LAST_UPDATE_TIMESTAMP'])
            
                # Riempimento dei campi a livello di prodotto
                list_prod_json = []
                
                for prodotto in df_senderlim_mock_tot_grouped.select("PRODUCT_TYPE").distinct().collect():
            
                    prodotto = prodotto.asDict()['PRODUCT_TYPE']
                    diz_prod_json = {}
                    diz_prod_json['id'] = prodotto
                    diz_prod_json['nome'] = prodotto
            
                    df_senderlim_mock_prod = df_senderlim_mock_tot_grouped.filter(F.col('PRODUCT_TYPE')==prodotto)
            
                    sum_monthly_estimate_prod = 0
                    for row in df_senderlim_mock_prod.select('MONTHLY_ESTIMATE').collect():
                        sum_monthly_estimate_prod += row['MONTHLY_ESTIMATE']
                    diz_prod_json['valore_totale'] = sum_monthly_estimate_prod
            
                    # Riempimento dei campi per nazione
                    diz_prod_json['varianti'] = []
                    
                    diz_naz_json_nz = {}
                    diz_naz_json_nz['codice'] = 'NZ'
                    diz_naz_json_nz['nome'] = 'NZ'
                    diz_naz_json_nz['valore_totale'] = sum_monthly_estimate_prod
                    diz_naz_json_nz['distribuzione'] = {'regionale':[]}
            
                    # Riempimento campi per regione
                    list_reg_json = []
            
                    for regione in df_senderlim_mock_prod.select('REGIONE').collect():
            
                        regione = regione.asDict()['REGIONE']
                        diz_reg_json = {}
                        diz_reg_json['regione'] = regione
                        diz_reg_json['province'] = None
            
                        sum_monthly_estimate_reg = 0
                        for row in df_senderlim_mock_prod.select('MONTHLY_ESTIMATE').filter(F.col('REGIONE')==regione).collect():
                            sum_monthly_estimate_reg += row['MONTHLY_ESTIMATE']
                        diz_reg_json['valore'] = sum_monthly_estimate_reg
            
                        list_reg_json.append(diz_reg_json)
            
                    diz_naz_json_nz['distribuzione'] = {'regionale': list_reg_json}
            
                    diz_prod_json['varianti'].append(diz_naz_json_nz)
                    
                    if prodotto == 'AR':
                        diz_naz_json_int = {}
                        diz_naz_json_int['codice'] = 'INT'
                        diz_naz_json_int['nome'] = 'INT'
                        diz_naz_json_int['valore_totale'] = 0
                        diz_naz_json_int['distribuzione'] = None
                        
                        diz_prod_json['varianti'].append(diz_naz_json_int)
            
                    list_prod_json.append(diz_prod_json)
            
                # Aggiunta del prodotto digitale
                diz_digitale = {
                        "id": "digitale",
                        "nome": "digitale",
                        "valore_totale": 0,
                        "varianti": [
                            {
                                "codice": "PEC",
                                "nome": "PEC",
                                "valore_totale": 0,
                                "distribuzione": None
                            }
                        ]
                    }
                
                list_prod_json.append(diz_digitale)
            
                diz_ente_json['prodotti'] = list_prod_json
                
                
                # Scrittura del Json dell'ente all'interno del file zip
                str_ente_json = json.dumps(diz_ente_json, indent=4, ensure_ascii=False)
                zipf.writestr(ente+".json", str_ente_json)
        

else:
    print('Nessuna commessa da lavorare')



##### Applicazione della INSERT_MOCK_SENDER_LIMITS: 1) GET_PRESIGNED_URL, 2) copia zip con destination_filename, 3) INSERT_MOCK_SENDER_LIMITS, 4) rimozione della copia
def lambda_presigned_url(lambda_delayer, source_filename):
        """
        Generiamo il presigned URL utilizzando la GET_PRESIGNED_URL
    
        Args:
            lambda_delayer (botocore.client.Lambda): connessione alla lambda
            source_filename (string): nome originale del file
    
        Returns:
            string: presigned URL che indica il bucket nel quale caricare i file
            string: nome del file che si aspetta la presigned URL al momento del caricamento
        """
        # GET_PRESIGNED_URL - testDelayerLambda
        payload_lambda={
            "operationType": "GET_PRESIGNED_URL",
            "parameters": {
                "fileName": source_filename,
                "checksumSha256B64": "abcd1234efgh5678ijkl9012mnop3456",
                "presignedUrlType": "UPLOAD"
            }
        }
        response_lambda=lambda_delayer.invoke(FunctionName='pn-testDelayerLambda',Payload=json.dumps(payload_lambda))
        read_response = response_lambda['Payload'].read()
        string_response = read_response.decode('utf-8')
        response_dict = json.loads(string_response)
        if response_dict['statusCode'] not in (200, 201, 204):
            raise Exception(response_dict['body'])
        response_dict_body = json.loads(response_dict['body'])
        uploadUrl = response_dict_body['uploadUrl']
        key = response_dict_body['key']
        print('GET_PRESIGNED_URL BODY: ',response_dict_body)
        print('GET_PRESIGNED_URL terminata con statuscode ',response_dict['statusCode'])
        return uploadUrl, key
    

class S3BodyWrapper:
    """
    Wrap dello stream originale leggendo i dati tramite la read() e recuperando la lunghezza del file tramite la __len__()    
    """
    def __init__(self, body, length):
        self.body = body
        self.length = length
    def read(self, amt=None):
        return self.body.read(amt)
    def __len__(self):
        return self.length

    
def lambda_insert_mock_sender_limits(lambda_delayer,filename):
    """
    Effettuiamo l'operazione di INSERT_MOCK_SENDER_LIMITS specificando il nome del file da importare per l'INSERT_MOCK_SENDER_LIMITS

    Args:
    lambda_delayer (botocore.client.Lambda): connessione alla lambda
    filename (string): nome del file da importare da dare in input all'operazione di INSERT_MOCK_SENDER_LIMITS
    """
    # INSERT_MOCK_SENDER_LIMITS - testDelayerLambda
    payload_lambda={
        "operationType": "INSERT_MOCK_SENDER_LIMITS",
        "parameters": [filename]
    }
    response_lambda=lambda_delayer.invoke(FunctionName='pn-testDelayerLambda',Payload=json.dumps(payload_lambda))
    read_response = response_lambda['Payload'].read()
    string_response = read_response.decode('utf-8')
    response_dict = json.loads(string_response)
    if response_dict['statusCode'] not in (200, 201, 204):
        raise Exception(response_dict['body'])
    print('INSERT_MOCK_SENDER_LIMITS terminata con statuscode ',response_dict['statusCode'])


def carica_oggetto(s3_client, s3_file_key, source_bucket):
    """
    Questa funzione gestisce le operazioni di GET_PRESIGNED_URL e INSERT_MOCK_SENDER_LIMITS, con le relative operazioni a corredo

    Args:
        s3_client (botocore.client.S3): connessione ad s3
        s3_file_key (string): chiave dell'oggetto sorgente da caricare nel presigned URL e conseguentemente importare tramite l'operazione di INSERT_MOCK_SENDER_LIMITS
        source_bucket (string): bucket di origine dell'oggetto sorgente

    Returns:
        string: nome del file oggetto della INSERT_MOCK_SENDER_LIMITS 
    """
    config = Config(read_timeout=900) # allungato a 15 minuti
    lambda_delayer = boto3.client('lambda',config=config)
    source_path = '/'.join(s3_file_key.split('/')[:-1])
    source_filename = s3_file_key.split('/')[-1]
    # GET PRESIGNED URL
    uploadUrl, destination_filename = lambda_presigned_url(lambda_delayer,source_filename)
    # otteniamo l'oggetto S3 come streaming body
    print(source_path)
    print(destination_filename)
    response = s3_client.get_object(Bucket=source_bucket, Key=source_path+'/'+destination_filename)
    body = response["Body"]
    size = response["ContentLength"]
    streaming_body = S3BodyWrapper(body, size)
    # upload dell'oggetto sul bucket S3 indicato dal presigned url
    put_response = requests.put(
        uploadUrl,
        data=streaming_body,
        timeout=300
    )
    if put_response.status_code not in (200, 201, 204):
        raise Exception(put_response.text)
    # INSERT_MOCK_SENDER_LIMITS
    lambda_insert_mock_sender_limits(lambda_delayer,destination_filename)
    

# Scrittura su S3
if pianificazione_postalizzazioni in ['Utilizza le commesse di default e le commesse di mock','Utilizza solo le commesse di mock']:
    
    print('Scrittura su S3')
    
    output_prefix = None
    s3_client = boto3.client('s3')
    target_date = date.today()
    
    for _ in range(120):  # limite di sicurezza a 30 gg
        input_prefix = target_date.strftime("%Y/%m/%d/")
        response = s3_client.list_objects_v2(
            Bucket=s3_bucket,
            Prefix='input/'+input_prefix+mese_simulazione[:7]+'/',
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
        s3_key = output_prefix + "dati_extra/commesse_mock/" + "ID_" + str(id_simulazione)
        path_finalpart = "s3://" + s3_bucket+"/" + s3_key


    zip_buffer.seek(0)
    print(path_finalpart + "/" + file_zip)
    s3_client.upload_fileobj(zip_buffer, s3_bucket, s3_key + "/" + file_zip)
    # s3_client.upload_file(tmp_path, s3_bucket, path_finalpart + "/" + file_zip)

    # Rimozione del file temporaneo
    # os.remove(tmp_path)
    
    
    print('INSERT_MOCK_SENDER_LIMITS')
    s3_file_key = s3_key + "/" + file_zip
    carica_oggetto(s3_client, s3_file_key, s3_bucket)


# da lasciare come ultimo comando per indicare che il job ha terminato con SUCCESS la sua esecuzione
job.commit()
