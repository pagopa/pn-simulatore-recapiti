"""
AWS Lambda che viene invocata come primo task della step function pn-simulatore-recapiti-sf-GestioneSimulazione e si occupa di creare l'istanza nella tabella del db denominata "SIMULAZIONE" per simulazioni automatizzate 

Trigger:
    Step function pn-simulatore-recapiti-sf-GestioneSimulazione

Input:
    tipo_simulazione: 'Automatizzata' o 'Manuale'
    id_simulazione_manuale: valorizzata con l'id di simulazione in caso di simulazione 'Manuale', vuota per simulazione 'Automatizzata'
    mese_simulazione: prima settimana del mese di simulazione, nel formato YYYY-MM-DD

Output:
    id_simulazione_automatizzata: id della simulazione creata sul db solo nel caso in cui tipo_simulazione=='Automatizzata', altrimenti torna '-'
"""
import json
import boto3
import pg8000
import io
import os
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

def lambda_handler(event, context):    
    if event['tipo_simulazione'] == 'Automatizzata':
        settimana_simulazione = event["mese_simulazione"][:7] # mese_simulazione è del formato YYYY-MM-DD ma a noi interessa solamente YYYY-MM
        # calcoliamo il datetime now
        start_timestamp_esecuzione_simulazione = datetime.now(ZoneInfo("Europe/Rome")).strftime('%Y-%m-%d %H:%M:%S')
        # recupero variabili d'ambiente
        secretsManager_SecretId = os.environ['secretsManager_SecretId']
        db_host = os.environ['DB_HOST']
        db_name = os.environ['DB_NAME']
        db_port = os.environ['DB_PORT']
        # recupero credenziali da SecretsManager
        creds = recupero_credenziali_db(secretsManager_SecretId)
        # connessione db
        conn = connessione_db(db_host, db_name, db_port, creds)
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
    
    elif event['tipo_simulazione'] == 'Manuale':
        id_simulazione_creata = '-'

    else:
        raise Exception('tipo_simulazione non conforme')

    return {'statusCode': 200, 'id_simulazione_automatizzata':id_simulazione_creata}
