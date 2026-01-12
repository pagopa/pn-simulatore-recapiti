import json
import boto3
import os
import requests
import pg8000
import io
import csv
from datetime import datetime, timezone
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

def get_data_from_db(id_simulazione):
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
    cur.execute(f'''
        SELECT "UNIFIED_DELIVERY_DRIVER","COD_SIGLA_PROVINCIA","CAPACITY","CAPACITY","ACTIVATION_DATE_FROM","ACTIVATION_DATE_TO","PRODUCT_890","PRODUCT_AR" 
        FROM public."CAPACITA_SIMULATE"
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

def upload_object_on_presigned_url(uploadUrl, db_rows):
    # format data pre-creazione csv da caricare
    for row in db_rows:
        product_list = ''
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

    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=';')
    writer.writerow(['unifiedDeliveryDriver','geoKey','capacity','peakCapacity','activationDateFrom','activationDateTo','products'])
    writer.writerows(db_rows)

    csv_file = buffer.getvalue().encode("utf-8")
    # upload dell'oggetto sul bucket S3 indicato dal presigned url
    put_response = requests.put(
        uploadUrl,
        data=csv_file,
        timeout=300
    )
    if put_response.status_code == 200:
        return 0
    else:
        return 1


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

def upload_csv_objects(s3_client, s3_file_key, source_bucket):
    config = Config(read_timeout=900) # allungato a 15 minuti
    lambda_delayer = boto3.client('lambda',config=config)
    source_key = '/'.join(s3_file_key.split('/')[:-1])
    source_filename = s3_file_key.split('/')[-1]
    date_per_import_data = s3_file_key.split('/')[-2]
    # GET PRESIGNED URL
    uploadUrl, destination_filename = get_presigned_url_from_lambda(lambda_delayer,source_filename)
    # creiamo una copia dell'oggetto (che poi elimineremo) con il nome indicato dalla GET PRESIGNED URL
    create_object_copy_on_s3(s3_client,source_bucket,s3_file_key,source_key,destination_filename)
    # otteniamo l'oggetto S3 come streaming body
    response = s3_client.get_object(Bucket=source_bucket, Key=source_key+'/'+destination_filename)
    body = response["Body"]
    size = response["ContentLength"]
    streaming_body = S3BodyWrapper(body, size)
    # upload dell'oggetto sul bucket S3 indicato dal presigned url
    put_response = requests.put(
        uploadUrl,
        data=streaming_body,
        timeout=300
    )
    if put_response.status_code not in (200, 201, 204):
        raise Exception(put_response.text)

    # INSERT MOCK CAPACITIES
    errori_presenti_insert = insert_mock_capacities_with_lambda(lambda_delayer,destination_filename)
    if errori_presenti_insert != 0:
        print("Errore durante l'operazione di INSERT_MOCK_CAPACITIES tramite la lambda pn-testDelayerLambda")
        return {'statusCode': 500, 'lista_file_csv_caricati': lista_file_csv_caricati, 'errori_presenti':errori_presenti_insert}

    # cancelliamo la copia dell'oggetto sul bucket di progetto
    s3_client.delete_object(Bucket=source_bucket, Key=source_key+'/'+destination_filename)

    return destination_filename


def lambda_handler(event, context):
    # recuperiamo il numero totale di file da caricare
    lista_file_da_caricare = event["output_lambda_ListaFileImportData"]['Payload']['lista_file_csv']
    numero_file_da_caricare = len(lista_file_da_caricare[0]['lista_file_csv_1']) + len(lista_file_da_caricare[1]['lista_file_csv_2']) + len(lista_file_da_caricare[2]['lista_file_csv_3']) + len(lista_file_da_caricare[3]['lista_file_csv_4']) + len(lista_file_da_caricare[4]['lista_file_csv_5'])
    # recuperiamo i destination_filename dei file importati con IMPORT_DATA
    lista_file_csv_caricati = []
    source_bucket = os.environ['source_bucket']
    s3_client = boto3.client('s3')
    objects = s3_client.list_objects_v2(Bucket=source_bucket, Prefix='StepFunction_ListaFileImportData/')
    for obj in objects.get("Contents", []):
        if obj["Key"][-4:] == '.csv':
            lista_file_csv_caricati.append({'destination_filename':obj["Key"].split('/')[-1]})
    if len(lista_file_csv_caricati) != numero_file_da_caricare:
        return {'statusCode': 500, 'lista_file_csv_caricati': lista_file_csv_caricati, 'errori_presenti':1}

    if event["tipo_simulazione"] == 'Manuale':
        # Manuale
        # recupero parametri d'ambiente dalla step function
        id_simulazione_manuale = event["id_simulazione_manuale"]
        mese_simulazione = event["mese_simulazione"]
        # recupero dati dal db per creare csv da dare alla INSERT_MOCK_CAPACITIES
        db_rows = get_data_from_db(id_simulazione_manuale)
        # inizializzazione connessione lambda
        lambda_delayer=boto3.client('lambda')
        source_filename = 'mock_capacities_id'+id_simulazione_manuale+'.csv'
        # GET PRESIGNED URL
        uploadUrl, destination_filename = get_presigned_url_from_lambda(lambda_delayer,source_filename)
        # upload file sul presigned url
        errori_presenti_upload = upload_object_on_presigned_url(uploadUrl, db_rows)
        if errori_presenti_upload != 0:
            print("Errore durante l'operazione di caricamento del file sul presigned url di s3")
            return {'statusCode': 500, 'lista_file_csv_caricati': lista_file_csv_caricati, 'errori_presenti':errori_presenti_upload}
        # INSERT MOCK CAPACITIES
        errori_presenti_insert = insert_mock_capacities_with_lambda(lambda_delayer,destination_filename)
        if errori_presenti_insert != 0:
            print("Errore durante l'operazione di INSERT_MOCK_CAPACITIES tramite la lambda pn-testDelayerLambda")
            return {'statusCode': 500, 'lista_file_csv_caricati': lista_file_csv_caricati, 'errori_presenti':errori_presenti_insert}
        
        # aggiungiamo il nome del file caricato tramite la insert mock capacities nella lista_file_csv_caricati
        lista_file_csv_caricati.append({'destination_filename': destination_filename})
        print("File csv delle capacità di mock inserito correttamente ed aggiunto alla lista_file_csv_caricati")
        
        # aggiungiamo le capacità di mock per cap, prodotte dal relativo job glue, recuperandole da s3
        objects = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=f'input/cap_capacities/id{id_simulazione_manuale}_{mese_simulazione}/')
        for obj in objects.get("Contents", []):
            if obj["Key"][-4:] == '.csv':
                # carichiamo i csv nella destinazione recuperata attraverso la GET_PRESIGNED_URL ed effettuiamo l'operazione di INSERT_MOCK_CAPACITIES
                destination_filename = upload_csv_objects(s3_client, obj["Key"], source_bucket)
                # aggiungiamo alla lista_file_csv_caricati il nome del csv dei cap caricato tramite la INSERT_MOCK_CAPACITIES
                lista_file_csv_caricati.append({'destination_filename':destination_filename})
                print(f"File csv {obj["Key"]} dei cap inserito correttamente ed aggiunto alla lista_file_csv_caricati")
        
        return {'statusCode': 200, 'lista_file_csv_caricati': lista_file_csv_caricati, 'errori_presenti':0}
            
    elif event['tipo_simulazione'] == 'Automatizzata':
        # Automatizzata
        return {'statusCode': 200, 'lista_file_csv_caricati': lista_file_csv_caricati, 'errori_presenti':0}
    
    else:
        raise Exception('tipo_simulazione non conforme')
