"""
AWS Lambda che viene invocata come ultimo task della step function pn-simulatore-recapiti-sf-GestioneSimulazione e si occupa di settare il campo STATO ('Lavorata' o 'Non completata') nella tabella del db denominata "SIMULAZIONE"

Trigger:
    Step function pn-simulatore-recapiti-sf-GestioneSimulazione

Input:
    tipo_simulazione: 'Automatizzata' o 'Manuale'
    id_simulazione_automatizzata: valorizzata con l'id di simulazione in caso di simulazione 'Automatizzata'
    id_simulazione_manuale: valorizzata con l'id di simulazione in caso di simulazione 'Manuale'
"""
import json
import boto3
import pg8000
import os

def recupero_credenziali_db(secretsManager_SecretId):
    """
    Recupera le credenziali di connessione al db salvate sul secret manager

    Args:
        secretsManager_SecretId (string): arn dell'istanza secret manager che contiene le credenziali del db

    Returns:
        dict: credenziali del db recuperate dal secret manager
    """
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=os.environ['secretsManager_SecretId'])
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

def rimozione_cartella_temporanea_s3():
    """
    Rimozione cartella temporanea su s3 contenente la lista dei file caricati con IMPORT_DATA
    """
    source_bucket = os.environ['source_bucket']
    s3_client = boto3.client('s3')
    objects = s3_client.list_objects_v2(Bucket=source_bucket, Prefix='StepFunction_ListaFileImportData')
    for obj in objects.get("Contents", []):
        s3_client.delete_object(Bucket=source_bucket, Key=obj["Key"])


def lambda_handler(event, context):
    # recuperiamo l'id_simulazione_automatizzata (se automatizzata dall'output della lambda pn-simulatore-recapiti-CreaSimulazioneAutomatizzataDB, se manuale dai parametri d'ambiente della step function)
    if event['tipo_simulazione'] == 'Automatizzata':
        id_simulazione = event["output_lambda_ConfigurazioneSimulazione"]['Payload']['id_simulazione_automatizzata']
    elif event['tipo_simulazione'] == 'Manuale':
        id_simulazione = event['id_simulazione_manuale']
    else:
        raise Exception('Nessun id_simulazione valorizzato')
    
    # in base all'output dei task precedenti capiamo se lo stato è 'Lavorata' o 'Non completata' -> la prima condizione ci fa capire che abbiamo effettuato tutti i RUN_ALGORITHM con successo, la seconda che le postalizzazioni sono state importate con successo tramite IMPORT_DATA 
    if 'output_lambda_RecuperoCapacitaDiProduzione' in event and len(event['output_lambda_ListaFileImportData']['Payload']['lista_file_csv'])!=0 and event['output_lambda_INSERTMOCKCAPACITIES']['Payload']['errori_presenti']==0:
        stato_simulazione = 'Lavorata'
    else:
        stato_simulazione = 'Non completata'
    
    # recupero variabili d'ambiente
    secretsManager_SecretId = os.environ['secretsManager_SecretId']
    db_host = os.environ['DB_HOST']
    db_name = os.environ['DB_NAME']
    db_port = os.environ['DB_PORT']
    # recupero credenziali da SecretsManager
    creds = recupero_credenziali_db(secretsManager_SecretId)
    # connessione db
    conn = connessione_db(db_host, db_name, db_port, creds)
    # modifica dello stato della simulazione sul db da "In lavorazione" a "Lavorata"/"Non completata"
    cur = conn.cursor()    
    cur.execute(f'''
        UPDATE public."SIMULAZIONE" SET "STATO"='{stato_simulazione}' WHERE "ID"={id_simulazione};
    ''')
    conn.commit()
    cur.close()
    conn.close()

    rimozione_cartella_temporanea_s3()

    print('statusCode: 200')
