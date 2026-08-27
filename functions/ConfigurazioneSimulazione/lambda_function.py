"""
AWS Lambda che viene invocata come primo task della step function pn-simulatore-recapiti-sf-GestioneSimulazione e si occupa di creare l'istanza nella tabella del db denominata "SIMULAZIONE" per simulazioni automatizzate 

Trigger:
    Step function pn-simulatore-recapiti-sf-GestioneSimulazione

Input:
    tipo_simulazione: 'Automatizzata' o 'Manuale'
    mese_simulazione: prima settimana del mese di simulazione, nel formato yyyy-MM-dd

Output:
    id_simulazione_automatizzata: id della simulazione creata sul db solo nel caso in cui tipo_simulazione=='Automatizzata', altrimenti torna '-'
    start_timestamp_simulazione: timestamp di starting della simulazione
    pianificazione_postalizzazioni: scelta dell'utente che, tramite la webapp, ha selezionato il tipo di pianificazione_postalizzazioni
    postalizzazioni_fuori_commessa: scelta dell'utente che, tramite la webapp, ha selezionato o meno la checkbox delle postalizzazioni fuori commessa
"""
import json
import boto3
import pg8000
import os
from datetime import datetime
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
    # calcoliamo il datetime now
    start_timestamp_simulazione = datetime.now(ZoneInfo("Europe/Rome")).strftime('%Y-%m-%d %H:%M:%S')
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
    if event['tipo_simulazione'] == 'Automatizzata':
        settimana_simulazione = event["mese_simulazione"][:7] # mese_simulazione è del formato yyyy-MM-dd ma a noi interessa solamente yyyy-MM
        # query
        cur.execute(    
        f'''
        INSERT INTO public."SIMULAZIONE" ("NOME","DESCRIZIONE","STATO","START_TIMESTAMP","MESE_SIMULAZIONE","TIPO_CAPACITA","TIPO_SIMULAZIONE") 
        VALUES ('Automatizzata {settimana_simulazione}','Pianificazione settimanale automatizzata {settimana_simulazione}','In lavorazione','{start_timestamp_simulazione}','{settimana_simulazione}','Produzione','Automatizzata') 
        RETURNING "ID";
        '''
        )
        id_simulazione_automatizzata = str(cur.fetchone()[0])
        conn.commit()
        # creiamo le variabili pianificazione_postalizzazioni e postalizzazioni_fuori_commessa assegnando un valore simbolicamente nullo
        pianificazione_postalizzazioni = '-'
        postalizzazioni_fuori_commessa = '-'
    
    elif event['tipo_simulazione'] == 'Manuale':
        id_simulazione = event['id_simulazione_manuale']
        id_simulazione_automatizzata = '-'
        # recuperiamo dal db pianificazione_postalizzazioni e postalizzazioni_fuori_commessa
        # query
        cur.execute(    
        f'''
            SELECT "PIANIFICAZIONE_POSTALIZZAZIONI","POSTALIZZAZIONI_FUORI_COMMESSA" FROM public."SIMULAZIONE" WHERE "ID"='{id_simulazione}'
        '''
        )
        pianificazione_postalizzazioni,postalizzazioni_fuori_commessa = cur.fetchone()
        # modifica dello stato della simulazione sul db su "In lavorazione" e aggiornamento START_TIMESTAMP
        cur.execute(f'''
            UPDATE public."SIMULAZIONE" 
            SET "STATO"='In lavorazione', "START_TIMESTAMP"='{start_timestamp_simulazione}'
            WHERE "ID"={id_simulazione};
        ''')
        conn.commit()

    else:
        raise Exception('tipo_simulazione non conforme')

    # chiusura connessione
    cur.close()
    conn.close()

    return {
        'statusCode': 200, 
        'id_simulazione_automatizzata': id_simulazione_automatizzata, 
        'start_timestamp_simulazione': start_timestamp_simulazione, 
        'pianificazione_postalizzazioni': pianificazione_postalizzazioni,
        'postalizzazioni_fuori_commessa': str(postalizzazioni_fuori_commessa)
    }
