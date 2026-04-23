"""
AWS Lambda che viene invocata come primo task della step function pn-simulatore-recapiti-sf-RecuperoDati-Weekly e si occupa di calcolare la prima settimana del mese dal quale partire per il recupero settimanale dei dati 

Trigger:
    Step function pn-simulatore-recapiti-sf-RecuperoDati-Weekly

Input:
    date_simulazione: lista di dizionari, dove il formato di ogni dizionario è: {"mese_simulazione": "YYYY-MM-DD"}

Output:
    dizionario dove 'date_simulazione' è una lista di dizionari ed il formato di ogni dizionario è: {"mese_simulazione": "YYYY-MM-DD"}
"""
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dateutil.relativedelta import relativedelta
import os
import boto3
import pg8000

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
    # recupero variabili d'ambiente
    secretsManager_SecretId = os.environ['secretsManager_SecretId']
    db_host = os.environ['DB_HOST']
    db_name = os.environ['DB_NAME']
    db_port = os.environ['DB_PORT']
    # recupero credenziali da SecretsManager
    creds = recupero_credenziali_db(secretsManager_SecretId)
    # connessione db
    conn = connessione_db(db_host, db_name, db_port, creds)
    # query
    cur = conn.cursor()
    cur.execute('DELETE FROM public."DECLARED_CAPACITY_DELTA";')
    conn.commit()
    cur.execute('DELETE FROM public."SENDER_LIMIT_DELTA";')
    conn.commit()
    cur.close()
    conn.close()
    try:
        # se al lancio della step function è stata specificata almeno una data nella lista date_simulazione prendiamo queste date, altrimenti calcoliamo la prima settimana del mese dal quale partire per il recupero settimanale dei dati
        date_simulazione = event["date_simulazione"]
        if len(date_simulazione) != 0:
            return {"date_simulazione":date_simulazione}
    except:
        # dalle variabili d'ambiente recuperiamo il valore relativo a quanti mesi in avanti vogliamo simulare
        mesi_in_avanti = int(os.environ["mesi_in_avanti"])
        # datetime now
        datetime_now = datetime.now(ZoneInfo("Europe/Rome")) + relativedelta(months=mesi_in_avanti)
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

        # formattiamo la data come "YYYY-MM-DD"
        date_simulazione = [
            {
                "mese_simulazione": str(prima_settimana_da_processare.date())
            }
        ]

        return {"date_simulazione":date_simulazione}
