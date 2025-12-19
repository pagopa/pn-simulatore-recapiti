import json
import boto3
import os
import pg8000
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def get_db_credentials(secretsManager_SecretId):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secretsManager_SecretId)
    response_SecretString = json.loads(response['SecretString'])
    return response_SecretString


def get_connection(db_host, db_name, db_port, creds):
    conn = pg8000.connect(
        host=db_host,
        database=db_name,
        user=creds["username"],
        password=creds["password"],
        port=db_port
    )
    return conn


def get_data_from_db(cur, query):
    cur.execute(query)
    rows = cur.fetchall()
    return rows


def get_used_capacity_from_lambda(lista_settimane_processate, lista_province, lista_recapitisti, id_simulazione):
    lambda_delayer = boto3.client('lambda')
    # inizializziamo la variabile che conterrà la lista di record da inserire sul db
    rows = []
    for settimana in lista_settimane_processate:
        for recapitista in lista_recapitisti:
            for provincia in lista_province:
                payload_lambda={
                    "operationType": "GET_USED_CAPACITY",
                    "parameters": ["pn-PaperDeliveryDriverUsedCapacitiesMock",list(recapitista)[0], provincia[0], settimana]
                }
                response_lambda=lambda_delayer.invoke(FunctionName='pn-testDelayerLambda',Payload=json.dumps(payload_lambda))
                read_response = response_lambda['Payload'].read()
                string_response = read_response.decode('utf-8')
                response_dict = json.loads(string_response)
                response_dict_body = json.loads(response_dict['body'])
                print(response_dict_body)
                if response_dict['statusCode'] in (200, 201, 204) and response_dict_body != {"message":"Item not found"}:
                    try:
                        activation_date_from_tmp = datetime.strptime(response_dict_body['deliveryDate'], "%Y-%m-%d")
                        activation_date_to = (activation_date_from_tmp + timedelta(days=6)).strftime("%Y-%m-%d")+' 23:59:59'
                        activation_date_from = activation_date_from_tmp.strftime("%Y-%m-%d")+' 00:00:00'
                    except:
                        activation_date_from = ''
                        activation_date_to = ''
                    rows.append([list(recapitista)[0],activation_date_from,activation_date_to,response_dict_body['declaredCapacity'],None,None,provincia[1],provincia[0],None,None,datetime.now(ZoneInfo("Europe/Rome")).strftime('%Y-%m-%d %H:%M:%S'),id_simulazione])
    
    return rows


def insert_capacities_into_db(cur, rows, destination_table):
    #query
    insert_query = f"""
        INSERT INTO public."{destination_table}" ("UNIFIED_DELIVERY_DRIVER","ACTIVATION_DATE_FROM","ACTIVATION_DATE_TO","CAPACITY","SUM_MONTHLY_ESTIMATE","SUM_WEEKLY_ESTIMATE","REGIONE","COD_SIGLA_PROVINCIA","PRODUCT_890","PRODUCT_AR","LAST_UPDATE_TIMESTAMP","SIMULAZIONE_ID")
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    cur.executemany(insert_query, rows)


def get_second_monday_mese_successivo(data_string):
    # from string to datetime
    d = datetime.strptime(data_string, "%Y-%m-%d")
    # aumentiamo il mese di 1
    if d.month == 12:
        anno = d.year + 1
        mese = 1
    else:
        anno = d.year
        mese = d.month + 1
    # primo giorno del mese successivo
    first = datetime(anno, mese, 1)
    # giorno della settimana (lunedì=0, ... domenica=6)
    weekday = first.weekday()
    # calcoliamo quanto manca al primo lunedì
    giorni_fino_lunedi = (7 - weekday) % 7
    # recuperiamo il primo lunedì
    primo_lunedi = first + timedelta(days=giorni_fino_lunedi)
    # recuperiamo il secondo lunedì
    secondo_lunedi = (primo_lunedi + timedelta(days=7)).strftime("%Y-%m-%d")
    return secondo_lunedi


def lambda_handler(event, context):
    # recupero variabili d'ambiente
    secretsManager_SecretId = os.environ['secretsManager_SecretId']
    db_host = os.environ['DB_HOST']
    db_name = os.environ['DB_NAME']
    db_port = os.environ['DB_PORT']
    lista_settimane_processate = event["output_lambda_CountResidui"]['Payload']['lista_settimane_processate']
    tipo_simulazione = event["tipo_simulazione"]
    # recupero credenziali da SecretsManager
    creds = get_db_credentials(secretsManager_SecretId)
    # connessione db
    conn = get_connection(db_host, db_name, db_port, creds)
    cur = conn.cursor()
    # recupero lista_province e lista_recapitisti dal db
    lista_province = get_data_from_db(cur, 'SELECT DISTINCT "COD_SIGLA_PROVINCIA","REGIONE" FROM public."CAP_PROV_REG";')
    lista_province = [( r[0], r[1] ) for r in lista_province] # lista di dizionari con chiave provincia e valore regione
    lista_recapitisti = get_data_from_db(cur, 'SELECT DISTINCT "UNIFIED_DELIVERY_DRIVER" FROM public."DECLARED_CAPACITY";')
    
    if tipo_simulazione == 'Automatizzata':
        # recupero parametro id_simulazione
        id_simulazione = event["output_lambda_ConfigurazioneSimulazione"]['Payload']['id_simulazione_automatizzata']
        # GET_USED_CAPACITY
        rows = get_used_capacity_from_lambda(lista_settimane_processate, lista_province, lista_recapitisti, id_simulazione)
        # scrittura sul db delle capacità recuperate con la GET_USED_CAPACITY
        insert_capacities_into_db(cur, rows, 'CAPACITA_SIMULATE')
        # commit dell'inserimento
        conn.commit()

    elif tipo_simulazione == 'Manuale':
        # recupero parametro id_simulazione
        id_simulazione = event['id_simulazione_manuale']
        # la GET_USED_CAPACITY la lanciamo dalla seconda settimana del mese successivo in poi per le settimane processate dalla run algorithm
        second_monday_mese_successivo = get_second_monday_mese_successivo(lista_settimane_processate[0])
        lista_settimane_per_getusedcapacity = []
        flag_settimane_da_considerare = False
        for singola_settimana in lista_settimane_processate:
            if second_monday_mese_successivo == singola_settimana:
                flag_settimane_da_considerare = True
            if flag_settimane_da_considerare:
                lista_settimane_per_getusedcapacity.append(singola_settimana)
        # GET_USED_CAPACITY
        rows = get_used_capacity_from_lambda(lista_settimane_per_getusedcapacity, lista_province, lista_recapitisti, id_simulazione)
        # scrittura sul db delle capacità recuperate con la GET_USED_CAPACITY
        insert_capacities_into_db(cur, rows, 'CAPACITA_SIMULATE_DELTA')
        # cancelliamo le capacità di default
        cur.execute(f'''
            DELETE FROM public."CAPACITA_SIMULATE" WHERE "SIMULAZIONE_ID"={id_simulazione} AND "ACTIVATION_DATE_TO" IS NULL;
        ''')
        conn.commit()
        cur.execute(
            '''
                MERGE INTO public."CAPACITA_SIMULATE"
                USING public."CAPACITA_SIMULATE_DELTA"
                ON (public."CAPACITA_SIMULATE"."UNIFIED_DELIVERY_DRIVER" = public."CAPACITA_SIMULATE_DELTA"."UNIFIED_DELIVERY_DRIVER"
                    AND public."CAPACITA_SIMULATE"."SIMULAZIONE_ID" = public."CAPACITA_SIMULATE_DELTA"."SIMULAZIONE_ID"
                    AND public."CAPACITA_SIMULATE"."ACTIVATION_DATE_FROM" = public."CAPACITA_SIMULATE_DELTA"."ACTIVATION_DATE_FROM"
                    AND public."CAPACITA_SIMULATE"."COD_SIGLA_PROVINCIA" = public."CAPACITA_SIMULATE_DELTA"."COD_SIGLA_PROVINCIA") 
                --When records are matched, update the records if there is any change
                WHEN MATCHED AND public."CAPACITA_SIMULATE"."LAST_UPDATE_TIMESTAMP" < public."CAPACITA_SIMULATE_DELTA"."LAST_UPDATE_TIMESTAMP" 
                THEN UPDATE SET 
                "UNIFIED_DELIVERY_DRIVER" = public."CAPACITA_SIMULATE_DELTA"."UNIFIED_DELIVERY_DRIVER", 
                "ACTIVATION_DATE_FROM" = public."CAPACITA_SIMULATE_DELTA"."ACTIVATION_DATE_FROM", "ACTIVATION_DATE_TO" = public."CAPACITA_SIMULATE_DELTA"."ACTIVATION_DATE_TO", 
                "CAPACITY" = public."CAPACITA_SIMULATE_DELTA"."CAPACITY", "SUM_MONTHLY_ESTIMATE" = public."CAPACITA_SIMULATE_DELTA"."SUM_MONTHLY_ESTIMATE", 
                "SUM_WEEKLY_ESTIMATE" = public."CAPACITA_SIMULATE_DELTA"."SUM_WEEKLY_ESTIMATE", "REGIONE" = public."CAPACITA_SIMULATE_DELTA"."REGIONE", 
                "COD_SIGLA_PROVINCIA" = public."CAPACITA_SIMULATE_DELTA"."COD_SIGLA_PROVINCIA", "PRODUCT_890" = public."CAPACITA_SIMULATE_DELTA"."PRODUCT_890", 
                "PRODUCT_AR" = public."CAPACITA_SIMULATE_DELTA"."PRODUCT_AR", "SIMULAZIONE_ID" = public."CAPACITA_SIMULATE_DELTA"."SIMULAZIONE_ID",
                "LAST_UPDATE_TIMESTAMP" = public."CAPACITA_SIMULATE_DELTA"."LAST_UPDATE_TIMESTAMP"
                WHEN NOT MATCHED 
                THEN INSERT ("UNIFIED_DELIVERY_DRIVER" , "ACTIVATION_DATE_FROM", "ACTIVATION_DATE_TO" , 
                "CAPACITY" , "SUM_MONTHLY_ESTIMATE", "SUM_WEEKLY_ESTIMATE", "REGIONE", 
                "COD_SIGLA_PROVINCIA", "PRODUCT_890" , "PRODUCT_AR" , "SIMULAZIONE_ID" ,"LAST_UPDATE_TIMESTAMP") 
                VALUES 
                (public."CAPACITA_SIMULATE_DELTA"."UNIFIED_DELIVERY_DRIVER", 
                public."CAPACITA_SIMULATE_DELTA"."ACTIVATION_DATE_FROM", public."CAPACITA_SIMULATE_DELTA"."ACTIVATION_DATE_TO", 
                public."CAPACITA_SIMULATE_DELTA"."CAPACITY", public."CAPACITA_SIMULATE_DELTA"."SUM_MONTHLY_ESTIMATE", 
                public."CAPACITA_SIMULATE_DELTA"."SUM_WEEKLY_ESTIMATE", public."CAPACITA_SIMULATE_DELTA"."REGIONE", 
                public."CAPACITA_SIMULATE_DELTA"."COD_SIGLA_PROVINCIA", public."CAPACITA_SIMULATE_DELTA"."PRODUCT_890", 
                public."CAPACITA_SIMULATE_DELTA"."PRODUCT_AR", public."CAPACITA_SIMULATE_DELTA"."SIMULAZIONE_ID",
                public."CAPACITA_SIMULATE_DELTA"."LAST_UPDATE_TIMESTAMP");
            '''
        )
        conn.commit()
        cur.execute('DELETE FROM public."CAPACITA_SIMULATE_DELTA";')
        conn.commit()
    else:
        raise Exception('Parametro tipo_simulazione non valorizzato')

    # chiusura connessione
    cur.close()
    conn.close()
   
    return {'statusCode': 200}
