"""
AWS Lambda che si occupa di effettuare l'operazione di IMPORT_DATA

Trigger:
    Step function pn-simulatore-recapiti-sf-GestioneSimulazione

Input:
    s3_file_key: file_key del bucket s3 di progetto dove si trova il file csv da importare tramite l'operazione di IMPORT_DATA

Output:
    destination_filename: nome del file importato tramite l'operazione di IMPORT_DATA
"""
import json
import boto3
import os
import requests
from botocore.config import Config


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
        "parameters": {
            "fileName": source_filename,
            "checksumSha256B64": "abcd1234efgh5678ijkl9012mnop3456",
            "presignedUrlType": "UPLOAD"
        }
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

def crea_copia_csv_s3(s3_client,bucket_s3,obj_key,source_path,destination_filename):
    """
    Creiamo una copia del file csv che successivamente importeremo tramite la IMPORT_DATA con il nome indicato dalla GET_PRESIGNED_URL

    Args:
        s3_client (botocore.client.S3): connessione ad s3
        bucket_s3 (string): bucket di interesse
        obj_key (string): chiave dell'oggetto sorgente
        source_path (string): chiave dell'oggetto sorgente senza nome file
        destination_filename (string): nome del file fornito dalla GET_PRESIGNED_URL

    """
    s3_client.copy_object(
        Bucket=bucket_s3,
        CopySource={"Bucket": bucket_s3, "Key": obj_key},
        Key=source_path + '/' + destination_filename
    )

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


def lambda_import_data(lambda_delayer,filename,date_per_import_data):
    """
    Effettuiamo l'operazione di IMPORT_DATA specificando il nome del file da importare e la data della settimana per l'IMPORT_DATA

    Args:
    lambda_delayer (botocore.client.Lambda): connessione alla lambda
    filename (string): nome del file da importare da dare in input all'operazione di IMPORT_DATA
    date_per_import_data (string): settimana, nel formato YYYY-MM-DD, da dare in input all'operazione di IMPORT_DATA
    """
    # IMPORT DATA - testDelayerLambda
    payload_lambda={
        "operationType": "IMPORT_DATA",
        "parameters": ["pn-DelayerPaperDeliveryMock", "pn-PaperDeliveryCountersMock", filename, date_per_import_data]
    }
    response_lambda=lambda_delayer.invoke(FunctionName='pn-testDelayerLambda',Payload=json.dumps(payload_lambda))
    read_response = response_lambda['Payload'].read()
    string_response = read_response.decode('utf-8')
    response_dict = json.loads(string_response)
    if response_dict['statusCode'] not in (200, 201, 204):
        raise Exception(response_dict['body'])

def carica_oggetto(s3_client, s3_file_key, source_bucket):
    """
    Questa funzione gestisce le operazioni di GET_PRESIGNED_URL e IMPORT_DATA, con le relative operazioni a corredo

    Args:
        s3_client (botocore.client.S3): connessione ad s3
        s3_file_key (string): chiave dell'oggetto sorgente da caricare nel presigned URL e conseguentemente importare tramite l'operazione di IMPORT_DATA
        source_bucket (string): bucket di origine dell'oggetto sorgente

    Returns:
        string: nome del file oggetto della IMPORT_DATA 
    """
    config = Config(read_timeout=900) # allungato a 15 minuti
    lambda_delayer = boto3.client('lambda',config=config)
    source_path = '/'.join(s3_file_key.split('/')[:-1])
    source_filename = s3_file_key.split('/')[-1]
    date_per_import_data = s3_file_key.split('/')[-2]
    # GET PRESIGNED URL
    uploadUrl, destination_filename = lambda_presigned_url(lambda_delayer,source_filename)
    # creiamo una copia dell'oggetto (che poi elimineremo) con il nome indicato dalla GET PRESIGNED URL
    crea_copia_csv_s3(s3_client,source_bucket,s3_file_key,source_path,destination_filename)
    # otteniamo l'oggetto S3 come streaming body
    response = s3_client.get_object(Bucket=source_bucket, Key=source_path+'/'+destination_filename)
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

    # IMPORT DATA
    lambda_import_data(lambda_delayer,destination_filename, date_per_import_data)

    # cancelliamo la copia dell'oggetto sul bucket di progetto
    s3_client.delete_object(Bucket=source_bucket, Key=source_path+'/'+destination_filename)

    return destination_filename


def lambda_handler(event, context):
    # recupero variabili d'ambiente
    source_bucket = os.environ['source_bucket']
    s3_file_key = event['s3_file_key']
    # inizializzazione connessione verso s3
    s3_client = boto3.client('s3')
    
    # carichiamo i csv nella destinazione recuperata attraverso la GET_PRESIGNED_URL ed effettuiamo l'operazione di IMPORT_DATA
    destination_filename = carica_oggetto(s3_client, s3_file_key, source_bucket)
    
    # salviamo i nomi dei destination_file su s3
    s3_client.put_object(
        Bucket=source_bucket,
        Key='StepFunction_ListaFileImportData/'+destination_filename
    )
        
    print(f'statusCode : 200, destination_filename:{destination_filename}')
