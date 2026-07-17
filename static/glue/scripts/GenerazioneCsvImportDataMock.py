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

df_senderlim = df_senderlim.filter(F.col('DELIVERY_DATE')==mese_simulazione).drop('ID')


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

df_senderlim_mock = df_senderlim_mock.filter(F.col('SIMULAZIONE_ID')==F.lit(id_simulazione)).drop('ID')
                                     


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
pianificazione_postalizzazioni = df_simulazione.select('PIANIFICAZIONE_POSTALIZZAZIONI').collect()[0]

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

    # Scrittura file
    tmp_dir = '/tmp'
    mese_simulazione_path = mese_simulazione[:4] + '_' + mese_simulazione[5:7]
    file_zip = "Commesse_enti_"+mese_simulazione_path+"_ID"+str(id_simulazione)+".zip"
    tmp_path = tmp_dir + "/" + file_zip

    # Eliminazione del file zip nel caso si trovi già all'interno della cartella
    tmp_list = os.listdir(tmp_dir)
    for el in tmp_list:
        if el==file_zip:
            os.remove(tmp_path)
            print('Pulizia della cartella temporanea effettuata')  

    print('Scrittura file')
    with zipfile.ZipFile(tmp_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
        
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
                diz_ente_json['periodo_riferimento'] = str(df_senderlim_reg.select("DELIVERY_DATE").collect()[0].asDict()['DELIVERY_DATE'])
                diz_ente_json['last_update'] = str(df_senderlim_ente.select("LAST_UPDATE_TIMESTAMP").collect()[0].asDict()['LAST_UPDATE_TIMESTAMP'])
            
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
    with zipfile.ZipFile(tmp_path, "a", compression=zipfile.ZIP_DEFLATED) as zipf:
        
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
                                                                 .select('ID_SIMULAZIONE','ATTEMPT','LAST_UPDATE_TIMESTAMP','PRODUCT_TYPE','PA_ID','DELIVERY_DATE','MONTHLY_ESTIMATE','CAP','COD_SIGLA_PROVINCIA','REGIONE','quota_reale','floor_val','resto')
                
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
                                                .select('ID_SIMULAZIONE','DELIVERY_DATE','PA_ID','MONTHLY_ESTIMATE','PRODUCT_TYPE','SUDDIVISIONE_GEOGRAFICA','ATTEMPT','LAST_UPDATE_TIMESTAMP','REGIONE')
                
                df_senderlim_mock_subnaz=df_senderlim_mock_ente.filter(F.col('SUDDIVISIONE_GEOGRAFICA')!='Italia')
                
                df_senderlim_mock_tot = df_senderlim_mock_naz.union(df_senderlim_mock_subnaz)

                # Aggregazione per regione
                df_senderlim_mock_tot_grouped = df_senderlim_mock_tot.groupBy('PA_ID','DELIVERY_DATE','ID_SIMULAZIONE','PRODUCT_TYPE','REGIONE')\
                                                                     .agg(F.sum('MONTHLY_ESTIMATE').alias('MONTHLY_ESTIMATE'), F.max('LAST_UPDATE_TIMESTAMP').alias('LAST_UPDATE_TIMESTAMP'))
                
                # CREAZIONE JSON
                
                diz_ente_json = {}
                
                # Riempimento dei campi a livello di ente
                diz_ente_json['idEnte'] = ente
                diz_ente_json['contractId'] = ente
                diz_ente_json['periodo_riferimento'] = str(df_senderlim_mock_tot_grouped.select("DELIVERY_DATE").collect()[0].asDict()['DELIVERY_DATE'])
                diz_ente_json['last_update'] = str(df_senderlim_mock_tot_grouped.select("LAST_UPDATE_TIMESTAMP").collect()[0].asDict()['LAST_UPDATE_TIMESTAMP'])
            
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
        

    # Scrittura su S3
    print('Scrittura su S3')
    id_timestamp=[["1"]]
    timestamp_df=spark.createDataFrame(id_timestamp,["id"])

    timestamp_df = timestamp_df.withColumn("current_timestamp_string",F.date_format(F.current_timestamp(), "yyyyMMdd"))

    anno_corrente = timestamp_df.collect()[0][1][:4]
    mese_corrente = timestamp_df.collect()[0][1][4:6]
    giorno_corrente = timestamp_df.collect()[0][1][6:8]

    anno_str = mese_simulazione[:4]
    mese_str = mese_simulazione[5:7]

    path_finalpart = "input/"  + anno_corrente + "/" \
                                                              + mese_corrente + "/" \
                                                              + giorno_corrente + "/" \
                                                              + str(anno_str) + "-" + str(mese_str) + "/"\
                                                              + "commesse_mock/"\
                                                              + "ID_" + str(id_simulazione)


    s3_client = boto3.client('s3')
    s3_client.upload_file(tmp_path, s3_bucket, path_finalpart + "/" + file_zip)

    # Rimozione del file temporaneo
    os.remove(tmp_path)
    
    
 
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
    with zipfile.ZipFile(tmp_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
        
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
                                                                 .select('ID_SIMULAZIONE','ATTEMPT','LAST_UPDATE_TIMESTAMP','PRODUCT_TYPE','PA_ID','DELIVERY_DATE','MONTHLY_ESTIMATE','CAP','COD_SIGLA_PROVINCIA','REGIONE','quota_reale','floor_val','resto')
                
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
                                                .select('ID_SIMULAZIONE','DELIVERY_DATE','PA_ID','MONTHLY_ESTIMATE','PRODUCT_TYPE','SUDDIVISIONE_GEOGRAFICA','ATTEMPT','LAST_UPDATE_TIMESTAMP','REGIONE')
                
                df_senderlim_mock_subnaz=df_senderlim_mock_ente.filter(F.col('SUDDIVISIONE_GEOGRAFICA')!='Italia')
                
                df_senderlim_mock_tot = df_senderlim_mock_naz.union(df_senderlim_mock_subnaz)

                # Aggregazione per regione
                df_senderlim_mock_tot_grouped = df_senderlim_mock_tot.groupBy('PA_ID','DELIVERY_DATE','ID_SIMULAZIONE','PRODUCT_TYPE','REGIONE')\
                                                                     .agg(F.sum('MONTHLY_ESTIMATE').alias('MONTHLY_ESTIMATE'), F.max('LAST_UPDATE_TIMESTAMP').alias('LAST_UPDATE_TIMESTAMP'))
                
                # CREAZIONE JSON
                
                diz_ente_json = {}
                
                # Riempimento dei campi a livello di ente
                diz_ente_json['idEnte'] = ente
                diz_ente_json['contractId'] = ente
                diz_ente_json['periodo_riferimento'] = str(df_senderlim_mock_tot_grouped.select("DELIVERY_DATE").collect()[0].asDict()['DELIVERY_DATE'])
                diz_ente_json['last_update'] = str(df_senderlim_mock_tot_grouped.select("LAST_UPDATE_TIMESTAMP").collect()[0].asDict()['LAST_UPDATE_TIMESTAMP'])
            
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
        

    # Scrittura su S3
    print('Scrittura su S3')
    id_timestamp=[["1"]]
    timestamp_df=spark.createDataFrame(id_timestamp,["id"])

    timestamp_df = timestamp_df.withColumn("current_timestamp_string",F.date_format(F.current_timestamp(), "yyyyMMdd"))

    anno_corrente = timestamp_df.collect()[0][1][:4]
    mese_corrente = timestamp_df.collect()[0][1][4:6]
    giorno_corrente = timestamp_df.collect()[0][1][6:8]

    anno_str = mese_simulazione[:4]
    mese_str = mese_simulazione[5:7]

    path_finalpart = "input/"  + anno_corrente + "/" \
                                                              + mese_corrente + "/" \
                                                              + giorno_corrente + "/" \
                                                              + str(anno_str) + "-" + str(mese_str) + "/"\
                                                              + "commesse_mock/"\
                                                              + "ID_" + str(id_simulazione)


    s3_client = boto3.client('s3')
    s3_client.upload_file(tmp_path, s3_bucket, path_finalpart + "/" + file_zip)

    # Rimozione del file temporaneo
    os.remove(tmp_path)
     

else:
    print('Nessuna commessa da lavorare')


# da lasciare come ultimo comando per indicare che il job ha terminato con SUCCESS la sua esecuzione
job.commit()
 