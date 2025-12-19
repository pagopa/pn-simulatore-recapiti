import json
import boto3
import os
import requests
from botocore.config import Config


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


def import_data_with_lambda(lambda_delayer,filename, date_per_import_data):
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

    # IMPORT DATA
    import_data_with_lambda(lambda_delayer,destination_filename, date_per_import_data)

    # cancelliamo la copia dell'oggetto sul bucket di progetto
    s3_client.delete_object(Bucket=source_bucket, Key=source_key+'/'+destination_filename)

    return destination_filename


def lambda_handler(event, context):
    # recupero variabili d'ambiente
    source_bucket = os.environ['source_bucket']
    s3_file_key = event['s3_file_key']
    # inizializzazione connessione verso s3
    s3_client = boto3.client('s3')
    
    # carichiamo i csv nella destinazione recuperata attraverso la GET_PRESIGNED_URL ed effettuiamo l'operazione di IMPORT_DATA
    destination_filename = upload_csv_objects(s3_client, s3_file_key, source_bucket)
    
    # salviamo i nomi dei destination_file su s3
    s3_client.put_object(
        Bucket=source_bucket,
        Key='StepFunction_ListaFileImportData/'+destination_filename
    )
        
    print(f'statusCode : 200, destination_filename:{destination_filename}')
