"""
AWS Lambda che viene invocata a valle del ciclo di RUN_ALGORITHM e recupera le capacità utilizzate andandole a salvare sul db

Trigger:
    Step function pn-simulatore-recapiti-sf-GestioneSimulazione

Input:
    lista_settimane_processate
    tipo_simulazione: 'Automatizzata' o 'Manuale'
    id_simulazione_automatizzata: valorizzata con l'id di simulazione in caso di simulazione 'Automatizzata'
    id_simulazione_manuale: valorizzata con l'id di simulazione in caso di simulazione 'Manuale'
"""
import json
import boto3
import os
import pg8000
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def recupero_credenziali_db(secretsManager_SecretId):
    """
    Recupera le credenziali di connessione al db salvate sul secret manager

    Args:
        secretsManager_SecretId (string): arn dell'istanza secret manager che contiene le credenziali del db

    Returns:
        dict: credenziali del db recuperate dal secret manager
    """
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secretsManager_SecretId)
    response_SecretString = json.loads(response['SecretString'])
    return response_SecretString


def connessione_db(db_host, db_name, db_port, creds):
    """
    Crea la connessione al db

    Args:
        db_host (string): server del db
        db_name (string): nome del db
        db_port (string): porta del db
        creds (string): contiene le credenziali del db recuperate dal secret manager

    Returns:
        pg8000.legacy.Connection: istanza di connessione al db
    """
    conn = pg8000.connect(
        host=db_host,
        database=db_name,
        user=creds["username"],
        password=creds["password"],
        port=db_port
    )
    return conn


def recupero_dati_db(cur, query):
    """
    Recupero dati sul db attraverso una query fornita in input

    Args:
        cur (pg8000.legacy.Cursor): cursor per query su db
        query (string): testo della query da inviare al db

    Returns:
        list: record db recuperati dalla query
    """
    cur.execute(query)
    rows = cur.fetchall()
    return rows


def recupero_used_capacity(lista_settimane_processate, lista_province, lista_recapitisti, id_simulazione, flag_default):
    """
    Recupero della capacità utilizzata dalla RUN_ALGORITHM, tramite la GET_USED_CAPACITY, e processing per conseguente salvataggio sul db

    Args:
        lista_settimane_processate (list): lista delle settimane processate dalla RUN_ALGORITHM
        lista_province (list of tuple): lista di tuple provincia-regione
        lista_recapitisti (list): lista dei recapitisti
        id_simulazione (string): identificativo univoco della simulazione sul db
        flag_default (bool): flag che indica se è stata utilizzata una capacità di BAU o di picco

    Returns:
        list: record db recuperati dalla query e processati in vista del conseguente salvataggio sul db
    """
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
                    rows.append([list(recapitista)[0],activation_date_from,activation_date_to,response_dict_body['declaredCapacity'],None,None,provincia[1],provincia[0],None,None,datetime.now(ZoneInfo("Europe/Rome")).strftime('%Y-%m-%d %H:%M:%S'),flag_default,id_simulazione])
    
    return rows


def inserimento_used_capacity_db(cur, rows, destination_table):
    """
    Inserimento delle capacità utilizzate dalla RUN_ALGORITHM nella destination_table sul db

    Args:
        cur (pg8000.legacy.Cursor): cursor per query su db
        rows (list): lista di record da inserire nella destination_table sul db
        destination_table (string): tabella all'interno della quale faremo l'inserimento
    """
    #query
    insert_query = f"""
        INSERT INTO public."{destination_table}" ("UNIFIED_DELIVERY_DRIVER","ACTIVATION_DATE_FROM","ACTIVATION_DATE_TO","CAPACITY","SUM_MONTHLY_ESTIMATE","SUM_WEEKLY_ESTIMATE","REGIONE","COD_SIGLA_PROVINCIA","PRODUCT_890","PRODUCT_AR","LAST_UPDATE_TIMESTAMP","SIMULAZIONE_ID")
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    cur.executemany(insert_query, rows)


def calcolo_secondo_lunedi_mese_successivo(data_string):
    """
    Calcoliamo il secondo lunedì del mese successivo rispetto alla data fornita in input

    Args:
        data_string (string): primo lunedì processato dalla RUN_ALGORITHM nel formato YYYY-MM-DD

    Returns:
        string: secondo lunedì del mese successivo nel formato YYYY-MM-DD
    """
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

def recupero_lista_settimane_per_getusedcapacity(lista_settimane_processate):
    """
    Calcoliamo per quali settimane, fra quelle processate, dobbiamo recuperare la used capacity -> da requisito, per simulazioni manuali, le settimane per le quali fare la getusedcapacity sono quelle dalla seconda del mese successivo in poi

    Args:
        lista_settimane_processate (list): ogni elemento della lista è una stringa con una data (nel formato YYYY-MM-DD) del lunedì della settimana processata dalla RUN_ALGORITHM

    Returns:
        list: settimane per le quali chiamare la GET_USED_CAPACITY
    """
    second_monday_mese_successivo = calcolo_secondo_lunedi_mese_successivo(lista_settimane_processate[0])
    lista_settimane_per_getusedcapacity = []
    flag_settimane_da_considerare = False
    for singola_settimana in lista_settimane_processate:
        if second_monday_mese_successivo == singola_settimana:
            flag_settimane_da_considerare = True
        if flag_settimane_da_considerare:
            lista_settimane_per_getusedcapacity.append(singola_settimana)
    return lista_settimane_per_getusedcapacity

def rimozione_capacity(cur, conn, id_simulazione):
    """
    Cancelliamo le capacità con ACTIVATION_DATE_TO null dalla tabella del db denominata "CAPACITA_SIMULATE"

    Args:
        cur (pg8000.legacy.Cursor): cursor per query su db
        conn (pg8000.legacy.Connection): istanza di connessione al db che ci serve per committare l'operazione di delete che effettuiamo
        id_simulazione (string): identificativo univoco della simulazione sul db
    """
    cur.execute(f'''
        DELETE FROM public."CAPACITA_SIMULATE" WHERE "SIMULAZIONE_ID"={id_simulazione} AND "ACTIVATION_DATE_TO" IS NULL;
    ''')
    conn.commit()

def merge_capacita_simulate_delta(cur, conn):
    """
    Merge fra le tabelle del db denominate "CAPACITA_SIMULATE" e "CAPACITA_SIMULATE_DELTA"

    Args:
        cur (pg8000.legacy.Cursor): cursor per query su db
        conn (pg8000.legacy.Connection): istanza di connessione al db che ci serve per committare l'operazione di delete che effettuiamo
    """
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
            "FLAG_DEFAULT" = public."CAPACITA_SIMULATE_DELTA"."FLAG_DEFAULT",
            "LAST_UPDATE_TIMESTAMP" = public."CAPACITA_SIMULATE_DELTA"."LAST_UPDATE_TIMESTAMP"
            WHEN NOT MATCHED 
            THEN INSERT ("UNIFIED_DELIVERY_DRIVER" , "ACTIVATION_DATE_FROM", "ACTIVATION_DATE_TO" , 
            "CAPACITY" , "SUM_MONTHLY_ESTIMATE", "SUM_WEEKLY_ESTIMATE", "REGIONE", 
            "COD_SIGLA_PROVINCIA", "PRODUCT_890" , "PRODUCT_AR" , "SIMULAZIONE_ID", "FLAG_DEFAULT", "LAST_UPDATE_TIMESTAMP") 
            VALUES 
            (public."CAPACITA_SIMULATE_DELTA"."UNIFIED_DELIVERY_DRIVER", 
            public."CAPACITA_SIMULATE_DELTA"."ACTIVATION_DATE_FROM", public."CAPACITA_SIMULATE_DELTA"."ACTIVATION_DATE_TO", 
            public."CAPACITA_SIMULATE_DELTA"."CAPACITY", public."CAPACITA_SIMULATE_DELTA"."SUM_MONTHLY_ESTIMATE", 
            public."CAPACITA_SIMULATE_DELTA"."SUM_WEEKLY_ESTIMATE", public."CAPACITA_SIMULATE_DELTA"."REGIONE", 
            public."CAPACITA_SIMULATE_DELTA"."COD_SIGLA_PROVINCIA", public."CAPACITA_SIMULATE_DELTA"."PRODUCT_890", 
            public."CAPACITA_SIMULATE_DELTA"."PRODUCT_AR", public."CAPACITA_SIMULATE_DELTA"."SIMULAZIONE_ID",
            public."CAPACITA_SIMULATE_DELTA"."FLAG_DEFAULT", public."CAPACITA_SIMULATE_DELTA"."LAST_UPDATE_TIMESTAMP");
        '''
    )
    conn.commit()
    cur.execute('DELETE FROM public."CAPACITA_SIMULATE_DELTA";')
    conn.commit()

def lambda_handler(event, context):
    # recupero variabili d'ambiente
    secretsManager_SecretId = os.environ['secretsManager_SecretId']
    db_host = os.environ['DB_HOST']
    db_name = os.environ['DB_NAME']
    db_port = os.environ['DB_PORT']
    lista_settimane_processate = event["output_lambda_CountResidui"]['Payload']['lista_settimane_processate']
    tipo_simulazione = event["tipo_simulazione"]
    # recupero credenziali da SecretsManager
    creds = recupero_credenziali_db(secretsManager_SecretId)
    # connessione db
    conn = connessione_db(db_host, db_name, db_port, creds)
    cur = conn.cursor()
    # recupero lista_province e lista_recapitisti dal db
    lista_province = recupero_dati_db(cur, 'SELECT DISTINCT "COD_SIGLA_PROVINCIA","REGIONE" FROM public."CAP_PROV_REG";')
    lista_province = [( r[0], r[1] ) for r in lista_province] # a partire dai record recuperati dal db creo una lista di tuple formate da provincia e regione
    lista_recapitisti = recupero_dati_db(cur, 'SELECT DISTINCT "UNIFIED_DELIVERY_DRIVER" FROM public."DECLARED_CAPACITY";')
    
    if tipo_simulazione == 'Automatizzata':
        # recupero parametro id_simulazione
        id_simulazione = event["output_lambda_ConfigurazioneSimulazione"]['Payload']['id_simulazione_automatizzata']
        # FLAG_DEFAULT -> per l'automatizzata va settato sempre a True su tutte le capacità recuperate
        flag_default = True
        # GET_USED_CAPACITY
        rows = recupero_used_capacity(lista_settimane_processate, lista_province, lista_recapitisti, id_simulazione, flag_default)
        # scrittura sul db delle capacità recuperate con la GET_USED_CAPACITY
        inserimento_used_capacity_db(cur, rows, 'CAPACITA_SIMULATE')
        # commit dell'inserimento
        conn.commit()

    elif tipo_simulazione == 'Manuale':
        # recupero parametro id_simulazione
        id_simulazione = event['id_simulazione_manuale']
        # la GET_USED_CAPACITY la lanciamo dalla seconda settimana del mese successivo in poi per le settimane processate dalla run algorithm
        lista_settimane_per_getusedcapacity = recupero_lista_settimane_per_getusedcapacity(lista_settimane_processate)
        # FLAG_DEFAULT -> se TIPO_CAPACITA=Picco settiamo False, altrimenti settiamo True
        cur.execute('''SELECT "TIPO_CAPACITA" from public."SIMULAZIONE" where "ID"={id_simulazione};''')
        tipo_capacita = cur.fetchall()
        if tipo_capacita[0][0] == 'Picco':
            flag_default = False
        else:
            flag_default = True
        # GET_USED_CAPACITY
        rows = recupero_used_capacity(lista_settimane_per_getusedcapacity, lista_province, lista_recapitisti, id_simulazione, flag_default)
        # scrittura sul db delle capacità recuperate con la GET_USED_CAPACITY
        inserimento_used_capacity_db(cur, rows, 'CAPACITA_SIMULATE_DELTA')
        # cancelliamo le capacità con ACTIVATION_DATE_TO null ed effettuiamo la merge fra CAPACITA_SIMULATE e CAPACITA_SIMULATE_DELTA
        rimozione_capacity(cur, conn, id_simulazione)
        merge_capacita_simulate_delta(cur, conn)
    else:
        raise Exception('Parametro tipo_simulazione non valorizzato')

    # chiusura connessione
    cur.close()
    conn.close()
   
    return {'statusCode': 200}
