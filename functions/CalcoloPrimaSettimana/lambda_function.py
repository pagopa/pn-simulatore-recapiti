"""
AWS Lambda che viene invocata come primo task della step function pn-simulatore-recapiti-sf-RecuperoDati-Weekly e si occupa di calcolare la prima settimana del mese dal quale partire per il recupero settimanale dei dati 

Trigger:
    Step function pn-simulatore-recapiti-sf-RecuperoDati-Weekly

Input:
    mese_simulazione: mese da simulare, formato yyyy-MM-dd

Output:
    dizionario dove 'mesi_recupero_dati' è una lista di dizionari ed il formato di ogni dizionario è: {"mese_simulazione": "yyyy-MM-dd"}
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


def recupero_prima_settimana_mese_da_processare(anno, mese):
    """
    Funzione che restituisce la data del primo lunedì di un anno-mese dato in input, seguendo i requisiti di progetto

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

def gestione_recupero_mesi_intermedi(mesi_in_avanti, anno_partenza, mese_partenza):
    """
    Funzione che restituisce la prima settimana di ogni mese a partire dall'anno-mese di partenza e dai mesi in avanti

    Args:
        mesi_in_avanti (int): mesi in avanti da simulare rispetto a quello corrente
        anno_partenza (int): anno di riferimento
        mese_partenza (int): mese di riferimento
    
    Returns:
        list: lista di dizionari (nel formato {"mese_simulazione": "yyyy-MM-dd"}) 
    """
    mesi_intermedi_recupero_dati = []
    for i in range(mesi_in_avanti):
        # calcoliamo il numero totale di mesi da aggiungere dall'inizio
        totale_mesi = (mese_partenza - 1) + i
        # se superiamo i 12 mesi complessivi aumentiamo l'anno e settiamo adeguatamente il mese
        anno_intermedio = anno_partenza + (totale_mesi // 12)
        mese_intermedio = (totale_mesi % 12) + 1
        prima_settimana_da_processare = recupero_prima_settimana_mese_da_processare(anno_intermedio, mese_intermedio)
        mesi_intermedi_recupero_dati.append({"mese_recupero_dati": prima_settimana_da_processare})
    return mesi_intermedi_recupero_dati

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
    # pulizia tabella "DECLARED_CAPACITY_DELTA"
    cur.execute('DELETE FROM public."DECLARED_CAPACITY_DELTA";')
    conn.commit()
    # pulizia tabella "SENDER_LIMIT_DELTA"
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
    mesi_recupero_dati = []
    try:
        # USE CASE 1: mese_simulazione specificato come parametro in input della step function
        mese_simulazione = event["mese_simulazione"]
        datetime_mese_simulazione = datetime.strptime(mese_simulazione, '%Y-%m-%d')
        mesi_di_differenza = (datetime_mese_simulazione.year - anno_partenza) * 12 + datetime_mese_simulazione.month - mese_partenza
        if mesi_di_differenza <= 0:
            mesi_in_avanti = 0
        else:
            mesi_in_avanti = mesi_di_differenza
        print(f'Il mese da simulare, specificato come input della StepFunction, è: {mese_simulazione}')
    except:
        # USE CASE 1: nessun mese_simulazione specificato come parametro in input della step function, quindi andiamo a calcolare il mese di simulazione ed i mesi per il recupero dei dati
        # dalle variabili d'ambiente recuperiamo il valore relativo a quanti mesi in avanti vogliamo simulare
        mesi_in_avanti = int(os.environ["mesi_in_avanti"])
        # dalle variabili d'ambiente recuperiamo il cutoff
        cutoff = os.environ['cutoff']
        # REQUISITO: se siamo dopo il cut-off (impostato tramite parametro modificabile) del mese corrente, bisogna processare il mese successivo
        if giorno_partenza > int(cutoff):
            # aumentiamo il mese di 1
            if mese_partenza == 12:
                anno_partenza = anno_partenza + 1
                mese_partenza = 1
            else:
                mese_partenza = mese_partenza + 1
        # calcoliamo il mese di simulazione considerando gli eventuali n mesi in avanti
        datetime_target = datetime.strptime(f'{anno_partenza}-{mese_partenza}-01', '%Y-%m-%d') + relativedelta(months=mesi_in_avanti)
        mese_simulazione = recupero_prima_settimana_mese_da_processare(datetime_target.year, datetime_target.month)
        print(f'Il mese da simulare, che è stato calcolato considerando mesi_in_avanti ({mesi_in_avanti}) e cutoff ({cutoff}), è: {mese_simulazione}')
    
    # sulla base del relativo parametro d'ambiente controlliamo se effettuare (o meno) il recupero dei dati intermedi nel caso in cui stiamo simulando n mesi in avanti. Nota: se stiamo recuperando dati nel passato non verranno recuperati i mesi intermedi
    if mesi_in_avanti > 0 and os.environ["recupero_mesi_intermedi"]=='True':
        mesi_recupero_dati.extend(gestione_recupero_mesi_intermedi(mesi_in_avanti, anno_partenza, mese_partenza))
        
    # aggiungiamo la data di simulazione (formato "yyyy-MM-dd") nella lista delle date per il recupero dati
    mesi_recupero_dati.append({"mese_recupero_dati": mese_simulazione})

    return {
        "mesi_recupero_dati": mesi_recupero_dati,
        "mese_simulazione": mese_simulazione 
    }