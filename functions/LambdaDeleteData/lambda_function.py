import json
import boto3
from botocore.config import Config
import time
import random


def lambda_handler(event, context):
    # recuperiamo il nome del file da dare in input alla DELETE_DATA, caricato precedente tramite IMPORT_DATA o INSERT_MOCK_CAPACITIES
    filename_DELETEDATA = event['destination_filename']

    # inizializziamo la connessione alla lambda
    config = Config(read_timeout=900) # allungato a 15 minuti
    lambda_delayer = boto3.client('lambda',config=config)

    # chiamiamo la DELETE_DATA dando in input il file caricato precedente tramite IMPORT_DATA o INSERT_MOCK_CAPACITIES
    payload_lambda={
        "operationType": "DELETE_DATA",
        "parameters": [
            "pn-DelayerPaperDeliveryMock",
            "pn-PaperDeliveryDriverUsedCapacitiesMock",
            "pn-PaperDeliveryUsedSenderLimitMock",
            "pn-PaperDeliveryCountersMock",
            filename_DELETEDATA
        ]
    }
    # waiting random tra 0 e 5 secondi per scaglionare le richieste di DELETE_DATA ed evitare l'errore di DynamoDB "Throughput exceeds the current capacity of your table or index"
    time.sleep(random.uniform(0, 10))
    response_lambda=lambda_delayer.invoke(FunctionName='pn-testDelayerLambda',Payload=json.dumps(payload_lambda))
    read_response = response_lambda['Payload'].read()
    string_response = read_response.decode('utf-8')
    response_dict = json.loads(string_response)
    if 'statusCode' in response_dict:
        if response_dict['statusCode'] == 200:
            print(f"Nome file: {filename_DELETEDATA} - response: {response_dict}")
        else:
            raise Exception(f"Nome file: {filename_DELETEDATA} - response: {response_dict}")
    else:
        raise Exception(f"Nome file: {filename_DELETEDATA} - Timeout: {response_dict}")
