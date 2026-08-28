"""
AWS Lambda che viene invocata come primo task della step function pn-simulatore-recapiti-sf-RecuperoDati-Weekly e si occupa di calcolare la prima settimana del mese dal quale partire per il recupero settimanale dei dati 

Trigger:
    Step function pn-simulatore-recapiti-sf-RecuperoDati-Weekly

Input:
    date_simulazione: lista di dizionari, dove il formato di ogni dizionario è: {"mese_simulazione": "yyyy-MM-dd"}

Output:
    dizionario dove 'date_simulazione' è una lista di dizionari ed il formato di ogni dizionario è: {"mese_simulazione": "yyyy-MM-dd"}
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


def recupero_settimana_da_processare_da_anno_mese(anno, mese):
    """
    Funzione che restituisce la data del primo lunedì di un anno-mese dato in input, seguendo i requisiti di sistema

    Args:
        anno (int): anno di riferimento
        mese (int): mese di riferimento

    Returns:
        str: stringa del timestamp.date() (formato yyyy-MM-dd) contenente il primo lunedì rispetto all'anno-mese di input
    """
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
    return str(prima_settimana_da_processare.date())


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
    # datetime now
    datetime_now = datetime.now(ZoneInfo("Europe/Rome"))
    anno_partenza = datetime_now.year
    mese_partenza = datetime_now.month
    giorno_partenza = datetime_now.day
    # CALCOLO MESE SIMULAZIONE + MESI RECUPERO DATI
    try:
        # se al lancio della step function è stata specificato il mese_simulazione consideriamo questo mese, altrimenti calcoliamo la prima settimana del mese dal quale partire per il recupero settimanale dei dati
        mese_simulazione = event["mese_simulazione"]
        mesi_recupero_dati = [{"mese_recupero_dati": mese_simulazione}]
        datetime_mese_simulazione = datetime.strptime(mese_simulazione, '%Y-%m-%d')
        if datetime_mese_simulazione > datetime_now and anno_partenza != datetime_mese_simulazione.year and mese_partenza != datetime_mese_simulazione.month and os.environ["recupero_mesi_intermedi"]=='True':
            
    except:
        # dalle variabili d'ambiente recuperiamo il valore relativo a quanti mesi in avanti vogliamo simulare
        mesi_in_avanti = int(os.environ["mesi_in_avanti"])
        # REQUISITO: se siamo dopo il cut-off (impostato tramite parametro modificabile) del mese corrente, bisogna processare il mese successivo
        if giorno_partenza > int(os.environ['cutoff']):
            # aumentiamo il mese di 1
            if mese_partenza == 12:
                anno_partenza = anno_partenza + 1
                mese_partenza = 1
            else:
                mese_partenza = mese_partenza + 1
        mesi_recupero_dati = []
        # sulla base del relativo parametro d'ambiente controlliamo se effettuare (o meno) il recupero dei dati intermedi nel caso in cui stiamo simulando n mesi in avanti
        if mesi_in_avanti!=0 and os.environ["recupero_mesi_intermedi"]=='True':
            for i in range(mesi_in_avanti):
                # calcoliamo il numero totale di mesi da aggiungere dall'inizio
                totale_mesi = (mese_partenza - 1) + i
                # se superiamo i 12 mesi complessivi aumentiamo l'anno e settiamo adeguatamente il mese
                anno_intermedio = anno_partenza + (totale_mesi // 12)
                mese_intermedio = (totale_mesi % 12) + 1
                prima_settimana_da_processare = recupero_settimana_da_processare_da_anno_mese(anno_intermedio, mese_intermedio)
                mesi_recupero_dati.append({"mese_recupero_dati": prima_settimana_da_processare})
        # recuperiamo la lista dei mesi per il recupero dati considerando gli n mesi in avanti
        datetime_target = datetime.strptime(f'{anno_partenza}-{mese_partenza}-01', '%Y-%m-%d') + relativedelta(months=mesi_in_avanti)
        anno_target = datetime_target.year
        mese_target = datetime_target.month
        mese_simulazione = recupero_settimana_da_processare_da_anno_mese(anno_target, mese_target)
        # aggiungiamo la data (formato "yyyy-MM-dd") nella lista delle date per il recupero dati
        mesi_recupero_dati.append({"mese_recupero_dati": mese_simulazione})

    return {
        "mesi_recupero_dati": mesi_recupero_dati,
        "mese_simulazione": mese_simulazione 
    }
