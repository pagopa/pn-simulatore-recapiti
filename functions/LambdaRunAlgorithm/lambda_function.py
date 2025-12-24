import json
import boto3
import os
from datetime import datetime, timedelta

def run_algorithm_with_lambda(settimana_da_processare, nome_tabella_capacity):
    lambda_delayer = boto3.client('lambda')
    # RUN ALGORITHM - testDelayerLambda
    payload_lambda={
        "operationType": "RUN_ALGORITHM",
        "parameters": [
            os.environ['TABLE_PAPER_DELIVERY_MOCK'],
            nome_tabella_capacity,
            os.environ['TABLE_DRIVER_USED_CAPACITIES_MOCK'],
            os.environ['TABLE_SENDER_LIMIT'],
            os.environ['TABLE_USED_SENDER_LIMIT_MOCK'],
            os.environ['TABLE_COUNTERS_MOCK'],
            os.environ['THRESHOLD_VALUE'],
            settimana_da_processare
        ]
    }
    response_lambda=lambda_delayer.invoke(FunctionName=os.environ['DELAYER_LAMBDA_NAME'],Payload=json.dumps(payload_lambda))
    read_response = response_lambda['Payload'].read()
    string_response = read_response.decode('utf-8')
    response_dict = json.loads(string_response)
    return response_dict['statusCode']
    

def lambda_handler(event, context):

    # se siamo nelle iterazioni successive alla prima recuperiamo l'ultima settimana processata dai parametri di output di questa stessa lambda nella precedente iterazione, altrimenti prendiamo la settimana dal parametro 'mese_simulazione'
    try:
        settimana_da_processare = event["output_lambda_RUNALGORITHM"]['Payload']['settimana_processata_RUN_ALGORITHM']
        settimana_da_processare = datetime.strptime(settimana_da_processare, "%Y-%m-%d")
        settimana_da_processare = (settimana_da_processare + timedelta(days=7)).strftime("%Y-%m-%d")
    except:
        settimana_da_processare = event['mese_simulazione']

    if event["tipo_simulazione"] == 'Automatizzata':
        nome_tabella_capacity = os.environ['TABLE_DRIVER_CAPACITIES']
    elif event["tipo_simulazione"] == 'Manuale':
        nome_tabella_capacity = os.environ['TABLE_DRIVER_CAPACITIES_MOCK']
    else:
        raise Exception('Parametro tipo_simulazione non valorizzato')

    # RUN_ALGORITHM
    statusCode = run_algorithm_with_lambda(settimana_da_processare, nome_tabella_capacity)
    # check statusCode dell'operazione RUN_ALGORITHM
    if statusCode == 200:
        return {'statusCode': 200, "settimana_processata_RUN_ALGORITHM": settimana_da_processare}
    else:
        return {'statusCode': 500, "settimana_processata_RUN_ALGORITHM": settimana_da_processare}

