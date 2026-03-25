"""
AWS Lambda che si occupa di controllare se ci sono stati errori nelle import effettuate nello step precedente e caricare le capacità per provincia e per CAP tramite la INSERT_MOCK_CAPACITIES (in caso di simulaziome Manuale)

Trigger:
    Step function pn-simulatore-recapiti-sf-GestioneSimulazione

Input:
    lista_file_csv: lista dei file assegnati all'IMPORT_DATA per il caricamento
    tipo_simulazione: 'Automatizzata' o 'Manuale'
    id_simulazione_manuale: valorizzata con l'id di simulazione in caso di simulazione 'Manuale'

Output:
    lista_file_csv_caricati: lista file caricati tramite IMPORT_DATA che servirà per la DELETE_DATA
    errori_presenti: 1 se vi sono errori, altrimenti 0
"""
import json
import boto3
import os
import requests
import pg8000
import io
import csv
from datetime import timezone, date, timedelta
from botocore.config import Config
import math

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

def recupero_dati_db(id_simulazione, tabella_sorgente):
    """
    Recupero dati sul db

    Args:
        id_simulazione (string): identificativo univoco della simulazione sul db
        tabella_sorgente (string): nome della tabella del db da interrogare

    Returns:
        list: record db recuperati dalla query
    """
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
    if tabella_sorgente == 'CAPACITA_SIMULATE':
        cur.execute(f'''
            SELECT "UNIFIED_DELIVERY_DRIVER","COD_SIGLA_PROVINCIA","CAPACITY","CAPACITY","ACTIVATION_DATE_FROM","ACTIVATION_DATE_TO","PRODUCT_890","PRODUCT_AR" 
            FROM public."{tabella_sorgente}"
            WHERE "SIMULAZIONE_ID"='{id_simulazione}'
        ''')
    else:
        cur.execute(f'''
            SELECT "UNIFIED_DELIVERY_DRIVER","GEOKEY","CAPACITY","PEAK_CAPACITY","ACTIVATION_DATE_FROM","ACTIVATION_DATE_TO"
            FROM public."{tabella_sorgente}"
            WHERE "SIMULAZIONE_ID"='{id_simulazione}'
        ''')
    rows = cur.fetchall()
    # chiusura connessione
    cur.close()
    conn.close()
    
    return rows


def lambda_presigned_url(lambda_delayer, source_filename):
    """
    Generiamo il presigned URL utilizzando la GET_PRESIGNED_URL

    Args:
        lambda_delayer (botocore.client.Lambda): connessione alla lambda
        source_filename (string): nome originale del file

    Returns:
        string: presigned URL che indica il bucket nel quale caricare i file
        string: nome del file che si aspetta la presigned URL al momento del caricamento
    """
    # GET_PRESIGNED_URL - testDelayerLambda
    payload_lambda={
        "operationType": "GET_PRESIGNED_URL",
        "parameters": [source_filename,"checksumSha256B64"]
    }
    response_lambda=lambda_delayer.invoke(FunctionName='pn-testDelayerLambda',Payload=json.dumps(payload_lambda))
    read_response = response_lambda['Payload'].read()
    string_response = read_response.decode('utf-8')
    response_dict = json.loads(string_response)
    if response_dict['statusCode'] not in (200, 201, 204):
        raise Exception(response_dict['body'])
    response_dict_body = json.loads(response_dict['body'])
    uploadUrl = response_dict_body['uploadUrl']
    key = response_dict_body['key']
    return uploadUrl, key

def caricamento_csv(db_rows, capacity_granularity, lambda_delayer, id_simulazione_manuale):
    """
    Questa funzione gestisce le operazioni di GET_PRESIGNED_URL e caricamento del file csv nel presigned url, con le relative operazioni a corredo

    Args:
        db_rows (list): lista dei record delle capacità recuperati dal db per creare il file csv da caricare nel presigned url 
        capacity_granularity (string): indica se le capacità sono a livello di provincia o a livello di CAP
        lambda_delayer (botocore.client.Lambda): connessione alla lambda
        id_simulazione_manuale (string): identificativo univoco della simulazione sul db

    Returns:
        int: esito operazioni (0 se non ci sono errori, 1 se ci sono errori)
        list: lista dei file csv caricati su s3 da inserire successivamenteo tramite la INSERT_MOCK_CAPACITIES
    """
    # elaborazione product e data pre-creazione csv da caricare
    for row in db_rows:
        # formattiamo il product
        product_list = ''
        if capacity_granularity=='province':
            if row[-2] == True:
                product_list += '890,'
            if row[-1] == True:
                product_list += 'AR,'
            product_list += 'RS'
            # rimuoviamo gli ultimi 2 elementi da ogni riga recuperata dal db (product_890 e product_AR)
            del row[-2]
            del row[-1]
        # aggiungiamo la stringa creata per il prodotto
        row.append(product_list)
        # adattiamo la data al formato ISO 8601 in UTC -> yyyy-mm-ddTHH:MM:SS.000Z
        row[4] = row[4].replace(tzinfo=timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        if row[5]!=None:
            row[5] = row[5].replace(tzinfo=timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

    if capacity_granularity=='province':
        source_filename = 'mock_capacities_id'+id_simulazione_manuale
    else:
        source_filename = 'mock_capacities_cap_id'+id_simulazione_manuale
    
    lista_file_csv_caricati_su_s3 = []
    max_rows = 10000
    num_chunks=math.ceil(len(db_rows)/max_rows)
    for index in range(num_chunks):
        chunk = db_rows[index * 10000:(index + 1) * 10000]
        # GET PRESIGNED URL
        uploadUrl, destination_filename = lambda_presigned_url(lambda_delayer,source_filename+f"_part_{index}.csv")
        buffer = io.StringIO()
        writer = csv.writer(buffer, delimiter=';')
        writer.writerow(['unifiedDeliveryDriver','geoKey','capacity','peakCapacity','activationDateFrom','activationDateTo','products'])
        writer.writerows(chunk)
        csv_file = buffer.getvalue().encode("utf-8")
        # upload dell'oggetto sul bucket S3 indicato dal presigned url
        put_response = requests.put(
            uploadUrl,
            data=csv_file,
            timeout=300
        )
        if put_response.status_code != 200:
            print("Errore durante l'operazione di caricamento del file sul presigned url di s3")
            return 1, lista_file_csv_caricati_su_s3
        else:
            lista_file_csv_caricati_su_s3.append(destination_filename)

    return 0, lista_file_csv_caricati_su_s3

def lambda_insert_mock_capacities(lambda_delayer,lista_filename_insertMockCapacities):
    """
    Importiamo le capacità attraverso l'operazione di INSERT_MOCK_CAPACITIES

    Args:
        lambda_delayer (botocore.client.Lambda): connessione alla lambda
        lista_filename_insertMockCapacities (list): lista filename da inserire tramite la INSERT_MOCK_CAPACITIES

    Returns:
        int: esito operazioni (0 se non ci sono errori, 1 se ci sono errori)
    """
    for filename in lista_filename_insertMockCapacities:
        # INSERT MOCK CAPACITIES - testDelayerLambda
        payload_lambda={
            "operationType": "INSERT_MOCK_CAPACITIES",
            "parameters": ["pn-PaperDeliveryDriverCapacitiesMock", filename]
        }
        response_lambda=lambda_delayer.invoke(FunctionName='pn-testDelayerLambda',Payload=json.dumps(payload_lambda))
        read_response = response_lambda['Payload'].read()
        string_response = read_response.decode('utf-8')
        response_dict = json.loads(string_response)
        if response_dict['statusCode'] != 200:
            print(response_dict)
            return 1
    return 0

class S3BodyWrapper:
    """
    Wrap dello stream originale leggendo i dati tramite la read() e recuperando la lunghezza del file tramite la __len__()    
    """
    def __init__(self, body, length):
        self.body = body
        self.length = length
    def read(self, amt=None):
        return self.body.read(amt)
    def __len__(self):
        return self.length


def gestione_insert_mock_capacities(capacity_granularity, id_simulazione_manuale, lambda_delayer):
    """
    Questa funzione gestisce le operazioni di recupero dati delle capacità dal db ed INSERT_MOCK_CAPACITIES, con le relative operazioni a corredo

    Args:
        capacity_granularity (string): indica se le capacità sono a livello di provincia o a livello di CAP
        id_simulazione_manuale (string): identificativo univoco della simulazione sul db
        lambda_delayer (botocore.client.Lambda): connessione alla lambda

    Returns:
        int: esito operazioni (0 se non ci sono errori, 1 se ci sono errori)
    """
    # recupero dati dal db per creare csv da dare alla INSERT_MOCK_CAPACITIES
    if capacity_granularity=='province':
        db_rows = recupero_dati_db(id_simulazione_manuale,'CAPACITA_SIMULATE')
    else:
        db_rows = recupero_dati_db(id_simulazione_manuale,'CAPACITA_SIMULATE_CAP')
    # upload file sul presigned url
    errori_presenti_upload, lista_filename_insertMockCapacities = caricamento_csv(db_rows, capacity_granularity, lambda_delayer, id_simulazione_manuale)
    if errori_presenti_upload != 0:
        print(f"Errore durante l'operazione di caricamento del file {capacity_granularity} sul presigned url di s3")
        return errori_presenti_upload
    # INSERT MOCK CAPACITIES
    errori_presenti_insert = lambda_insert_mock_capacities(lambda_delayer,lista_filename_insertMockCapacities)
    if errori_presenti_insert != 0:
        print(f"Errore durante l'operazione di INSERT_MOCK_CAPACITIES su {capacity_granularity} tramite la lambda pn-testDelayerLambda")
        return errori_presenti_insert
    return 0

def lambda_handler(event, context):
    # recuperiamo il numero totale di file assegnati alla IMPORT_DATA per il caricamento
    lista_file_da_caricare = event["output_lambda_ListaFileImportData"]['Payload']['lista_file_csv']
    numero_file_da_caricare = len(lista_file_da_caricare[0]['lista_file_csv_1']) + len(lista_file_da_caricare[1]['lista_file_csv_2']) + len(lista_file_da_caricare[2]['lista_file_csv_3']) + len(lista_file_da_caricare[3]['lista_file_csv_4']) + len(lista_file_da_caricare[4]['lista_file_csv_5']) + len(lista_file_da_caricare[5]['lista_file_csv_6'])
    # recupero parametri d'ambiente dalla step function
    source_bucket = os.environ['source_bucket']
    # inizializzazione connessione verso s3
    s3_client = boto3.client('s3')
    # inizializzazione connessione lambda
    lambda_delayer=boto3.client('lambda')
    # recuperiamo la lista dei file effettivamente caricati tramite IMPORT_DATA
    lista_file_csv_caricati = []
    objects = s3_client.list_objects_v2(Bucket=source_bucket, Prefix='StepFunction_ListaFileImportData/')
    for obj in objects.get("Contents", []):
        if obj["Key"][-4:] == '.csv':
            lista_file_csv_caricati.append({'destination_filename':obj["Key"].split('/')[-1]})
    # controlliamo se il numero di file assegnati alla IMPORT_DATA per il caricamento è uguale al numero di file effettivamente caricati tramite IMPORT_DATA
    if len(lista_file_csv_caricati) != numero_file_da_caricare:
        print(f"Il numero di file da caricare non coincide con il numero di file effettivamente caricati tramite IMPORT_DATA (lista_file_csv_caricati: {lista_file_csv_caricati}, numero_file_da_caricare: {numero_file_da_caricare})")
        return {'statusCode': 500, 'lista_file_csv_caricati': lista_file_csv_caricati, 'errori_presenti':1}

    if event["tipo_simulazione"] == 'Manuale':
        # Manuale
        # recupero parametri d'ambiente dalla step function
        id_simulazione_manuale = event["id_simulazione_manuale"]

        errori_presenti_insert_province = gestione_insert_mock_capacities('province', id_simulazione_manuale, lambda_delayer)
        if errori_presenti_insert_province != 0:
            return {'statusCode': 500, 'lista_file_csv_caricati': lista_file_csv_caricati, 'errori_presenti':errori_presenti_insert_province}

        errori_presenti_insert_cap = gestione_insert_mock_capacities('CAP', id_simulazione_manuale, lambda_delayer)
        if errori_presenti_insert_cap != 0:
            return {'statusCode': 500, 'lista_file_csv_caricati': lista_file_csv_caricati, 'errori_presenti':errori_presenti_insert_cap}

        print(f"File csv delle capacità di mock per provincia e per CAP inseriti correttamente tramite INSERT_MOCK_CAPACITIES")
        return {'statusCode': 200, 'lista_file_csv_caricati': lista_file_csv_caricati, 'errori_presenti':0}
            
    elif event['tipo_simulazione'] == 'Automatizzata':
        # Automatizzata
        return {'statusCode': 200, 'lista_file_csv_caricati': lista_file_csv_caricati, 'errori_presenti':0}

    else:
        raise Exception('Nessun id_simulazione valorizzato')