import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
import boto3
import pg8000

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
    # recupero credenziali da SecretsManager
    creds = get_db_credentials(secretsManager_SecretId)
    # connessione db
    conn = get_connection(db_host, db_name, db_port, creds)
    # query
    cur = conn.cursor()
    cur.execute('DELETE FROM public."DECLARED_CAPACITY_DELTA";')
    conn.commit()
    cur.execute('DELETE FROM public."SENDER_LIMIT_DELTA";')
    conn.commit()
    cur.close()
    conn.close()
    try:
        # se al lancio della step function è stata specificata la date_simulazione
        date_simulazione = event["date_simulazione"]
        if len(date_simulazione) != 0:
            return {"date_simulazione":date_simulazione}
    except:
        # datetime now
        datetime_now = datetime.now(ZoneInfo("Europe/Rome"))
        giorno = datetime_now.day
        mese = datetime_now.month
        anno = datetime_now.year

        # REQUISITO: dopo il cut-off (impostato tramite parametro modificabile) del mese corrente bisogna processare il mese successivo
        
        if giorno > int(os.environ['cutoff']):
            # aumentiamo il mese di 1
            if mese == 12:
                anno = anno + 1
                mese = 1
            else:
                mese = mese + 1
        # primo giorno del mese
        first = datetime(anno, mese, 1)
        # giorno della settimana (lunedì=0, ... domenica=6)
        weekday = first.weekday()
        # calcoliamo quanto manca al primo lunedì
        giorni_fino_lunedi = (7 - weekday) % 7
        # recuperiamo il primo lunedì
        prima_settimana_da_processare = first + timedelta(days=giorni_fino_lunedi)
        # se il primo lunedì del mese è 1, prendiamo l'8 come prima settimana da processare
        if prima_settimana_da_processare.day == 1:
            prima_settimana_da_processare = prima_settimana_da_processare + timedelta(days=7)

        # formattiamo la data come "yyyy-mm-dd"
        date_simulazione = [
            {
                "mese_simulazione": str(prima_settimana_da_processare.date())
            }
        ]

        return {"date_simulazione":date_simulazione}
