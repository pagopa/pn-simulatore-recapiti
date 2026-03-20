import json
import boto3
import os
import requests
import pg8000
import io
import csv
from datetime import timezone, date, timedelta
from botocore.config import Config

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

def get_data_from_db(id_simulazione, tabella_sorgente):
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


def get_presigned_url_from_lambda(lambda_delayer, source_filename):
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

def upload_object_on_s3(db_rows, capacity_granularity, lambda_delayer, id_simulazione_manuale):
    # format data pre-creazione csv da caricare
    for row in db_rows:
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

    num_chunks = (len(db_rows) + 10000 - 1)
    if capacity_granularity=='province':
        source_filename = 'mock_capacities_id'+id_simulazione_manuale
    else:
        source_filename = 'mock_capacities_cap_id'+id_simulazione_manuale
    
    lista_file_csv_caricati_su_s3 = []

    for index in range(num_chunks):
        chunk = db_rows[index * 10000:(index + 1) * 10000]
        # GET PRESIGNED URL
        uploadUrl, destination_filename = get_presigned_url_from_lambda(lambda_delayer,source_filename+f"_part_{index}.csv")
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

def insert_mock_capacities_with_lambda(lambda_delayer,filename):
    # INSERT MOCK CAPACITIES - testDelayerLambda
    payload_lambda={
        "operationType": "INSERT_MOCK_CAPACITIES",
        "parameters": ["pn-PaperDeliveryDriverCapacitiesMock", filename]
    }
    response_lambda=lambda_delayer.invoke(FunctionName='pn-testDelayerLambda',Payload=json.dumps(payload_lambda))
    read_response = response_lambda['Payload'].read()
    string_response = read_response.decode('utf-8')
    response_dict = json.loads(string_response)
    if response_dict['statusCode'] == 200:
        return 0
    else:
        return 1

def create_object_copy_on_s3(s3_client,source_bucket,obj_key,source_key,destination_filename):
    s3_client.copy_object(
        Bucket=source_bucket,
        CopySource={"Bucket": source_bucket, "Key": obj_key},
        Key=source_key + '/' + destination_filename
    )

class S3BodyWrapper:
    """Wrapper per StreamingBody che fornisce len() a requests"""
    def __init__(self, body, length):
        self.body = body
        self.length = length
    def read(self, amt=None):
        return self.body.read(amt)
    def __len__(self):
        return self.length


def get_ultima_data_estrazione(bucket_name, s3_client):
    target_date = date.today()

    for _ in range(30):  # limite di sicurezza a 30 gg
        prefix = target_date.strftime("%Y/%m/%d/")
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix='input/'+prefix,
            MaxKeys=1
        )
        # se la cartella esiste, ritorno la data
        if 'Contents' in response:
            return prefix
        # altrimenti vado al giorno precedente
        target_date -= timedelta(days=1)
    # se non viene trovata alcuna cartella corrispondente
    raise Exception("Nessuna folder input/yyyy/mm/dd_di_estrazione su S3 creata negli ultimi 30 gg")

def gestione_insert_mock_capacities(capacity_granularity, id_simulazione_manuale, lambda_delayer):
    # recupero dati dal db per creare csv da dare alla INSERT_MOCK_CAPACITIES
    if capacity_granularity=='province':
        db_rows = get_data_from_db(id_simulazione_manuale,'CAPACITA_SIMULATE')
    else:
        db_rows = get_data_from_db(id_simulazione_manuale,'CAPACITA_SIMULATE_CAP')
    # upload file sul presigned url
    errori_presenti_upload, lista_filename_insertMockCapacities = upload_object_on_s3(db_rows, capacity_granularity, lambda_delayer, id_simulazione_manuale)
    if errori_presenti_upload != 0:
        print(f"Errore durante l'operazione di caricamento del file {capacity_granularity} sul presigned url di s3")
        return errori_presenti_upload
    # INSERT MOCK CAPACITIES
    errori_presenti_insert = insert_mock_capacities_with_lambda(lambda_delayer,lista_filename_insertMockCapacities)
    if errori_presenti_insert != 0:
        print(f"Errore durante l'operazione di INSERT_MOCK_CAPACITIES su {capacity_granularity} tramite la lambda pn-testDelayerLambda")
        return errori_presenti_insert
    return 0

def lambda_handler(event, context):
    # recuperiamo il numero totale di file da caricare
    lista_file_da_caricare = event["output_lambda_ListaFileImportData"]['Payload']['lista_file_csv']
    numero_file_da_caricare = len(lista_file_da_caricare[0]['lista_file_csv_1']) + len(lista_file_da_caricare[1]['lista_file_csv_2']) + len(lista_file_da_caricare[2]['lista_file_csv_3']) + len(lista_file_da_caricare[3]['lista_file_csv_4']) + len(lista_file_da_caricare[4]['lista_file_csv_5']) + len(lista_file_da_caricare[5]['lista_file_csv_6'])
    # recupero parametri d'ambiente dalla step function
    source_bucket = os.environ['source_bucket']
    # inizializzazione connessione verso s3
    s3_client = boto3.client('s3')
    # inizializzazione connessione lambda
    lambda_delayer=boto3.client('lambda')
    # recuperiamo la lista dei file caricati tramite IMPORT DATA
    lista_file_csv_caricati = []
    objects = s3_client.list_objects_v2(Bucket=source_bucket, Prefix='StepFunction_ListaFileImportData/')
    for obj in objects.get("Contents", []):
        if obj["Key"][-4:] == '.csv':
            lista_file_csv_caricati.append({'destination_filename':obj["Key"].split('/')[-1]})
    # controlliamo se il numero di file da caricare è uguale al numero di file effettivamente caricati tramite IMPORT DATA
    if len(lista_file_csv_caricati) != numero_file_da_caricare:
        print(f"Il numero di file da caricare non coincide con il numero di file effettivamente caricati tramite IMPORT DATA (lista_file_csv_caricati: {lista_file_csv_caricati}, numero_file_da_caricare: {numero_file_da_caricare})")
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