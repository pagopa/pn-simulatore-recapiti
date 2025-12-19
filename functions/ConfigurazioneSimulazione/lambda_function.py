import json
import boto3
import pg8000
import io
import os
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

def lambda_handler(event, context):
    # recupero variabili d'ambiente
    secretsManager_SecretId = os.environ['secretsManager_SecretId']
    db_host = os.environ['DB_HOST']
    db_name = os.environ['DB_NAME']
    db_port = os.environ['DB_PORT']
    tipo_simulazione = event['tipo_simulazione']
    settimana_simulazione = event["mese_simulazione"]
    # calcoliamo il datetime now
    datetime_now = datetime.now(ZoneInfo("Europe/Rome"))

    
    if tipo_simulazione == 'Automatizzata':
        settimana_simulazione = settimana_simulazione[:7] # mese_simulazione è del formato yyyy-mm-dd ma a noi interessa solamente yyyy-mm
        start_timestamp_esecuzione_simulazione = datetime_now.strftime('%Y-%m-%d %H:%M:%S')
        # recupero credenziali da SecretsManager
        creds = get_db_credentials(secretsManager_SecretId)
        # connessione db
        conn = get_connection(db_host, db_name, db_port, creds)
        cur = conn.cursor()
        # query
        cur.execute(    
        f'''
        INSERT INTO public."SIMULAZIONE" ("NOME","DESCRIZIONE","STATO","TIMESTAMP_ESECUZIONE","MESE_SIMULAZIONE","TIPO_CAPACITA","TIPO_SIMULAZIONE") 
        VALUES ('Automatizzata {settimana_simulazione}','Pianificazione settimanale automatizzata {settimana_simulazione}','In lavorazione','{start_timestamp_esecuzione_simulazione}','{settimana_simulazione}','Produzione','Automatizzata') 
        RETURNING "ID";
        '''
        )
        id_simulazione_creata = str(cur.fetchone()[0])
        conn.commit()
        # chiusura connessione
        cur.close()
        conn.close()
    
    elif tipo_simulazione == 'Manuale':
        id_simulazione_creata = ''

    else:
        raise Exception('tipo_simulazione non conforme')

    return {'statusCode': 200, 'id_simulazione_automatizzata':id_simulazione_creata}
