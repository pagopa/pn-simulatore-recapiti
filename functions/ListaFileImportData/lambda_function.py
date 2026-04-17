"""
AWS Lambda che si occupa di recuperare la lista dei file da importare tramite l'operazione IMPORT_DATA

Trigger:
    Step function pn-simulatore-recapiti-sf-GestioneSimulazione

Input:
    settimana_processata_RUN_ALGORITHM: ultima settimana processata tramite l'operazione di RUN_ALGORITHM, nel formato YYYY-MM-DD, utile per calcolare la successiva settimana da processare
    mese_simulazione: prima settimana del mese di simulazione, nel formato YYYY-MM-DD
    tipo_simulazione: 'Automatizzata' o 'Manuale'

Output:
    lista_file_csv: fornisce alla LambdaInsertMockCapacities la lista dei file assegnati all'IMPORT_DATA per il caricamento
"""
import json
import boto3
from botocore.config import Config
import os
from datetime import date, timedelta


def recupero_ultima_data_estrazione(bucket_name, s3_client):
    """
    Recuperiamo la data dell'ultimo recupero dati sottoforma di prefisso del bucket s3 di progetto

    Args:
        bucket_name (string): nome del bucket s3 di progetto
        s3_client (botocore.client.S3): connessione ad s3

    Returns:
        string: prefisso del bucket fino alla cartella contenente i file che verranno successivamente importati tramite l'operazione di IMPORT_DATA
    """
    target_date = date.today()

    for _ in range(30):  # limite di sicurezza a 30 gg
        prefix = target_date.strftime("%Y/%m/%d/")
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix='input/'+prefix,
            MaxKeys=1
        )
        # se la cartella esiste, ritorno la key fino alla data dell'ultimo recupero dati
        if 'Contents' in response:
            return 'input/'+prefix
        # altrimenti vado al giorno precedente
        target_date -= timedelta(days=1)
    # se non viene trovata alcuna cartella corrispondente
    raise Exception("Nessuna folder input/YYYY/MM/DD_di_estrazione su S3 creata negli ultimi 30 gg")

def recupero_residui(prefix_s3,id_simulazione):
    """
    Recuperiamo i residui grazie all'operazione GET_RESIDUAL_PAPERS per la settimana precedente a quella di simulazione da aggiungere alla prima settimana di esecuzione (tranne nel caso in cui il giorno 1 del mese di simulazione è un lunedì)

    Args:
        prefix_s3 (string): prefisso del bucket fino alla cartella dove andremo a depositare il csv dei residui 
        id_simulazione (string): identificativo univoco della simulazione sul db
    
    Returns:
        filekey_csv_residui (string): key del file csv contenente i residui
    """
    config = Config(read_timeout=900) # allungato a 15 minuti
    lambda_delayer = boto3.client('lambda',config=config)

    # chiamiamo la GET_RESIDUAL_PAPERS dando in input 2 date: deliveryDate e executionDate
    payload_lambda={
        "operationType": "GET_RESIDUAL_PAPERS",
        "parameters": ["pn_delayer_paper_delivery_json_view", "2026-03-24", "2026-04-03"]
    }
    # gestione risposta GET_RESIDUAL_PAPERS
    response_lambda=lambda_delayer.invoke(FunctionName='pn-testDelayerLambda',Payload=json.dumps(payload_lambda))
    read_response = response_lambda['Payload'].read()
    string_response = read_response.decode('utf-8')
    response_dict = json.loads(string_response)
    if 'statusCode' in response_dict:
        if response_dict['statusCode'] == 200:
            print("Ci sono residui!")
        else:
            print(f"Non ci sono residui, statusCode: {response_dict['statusCode']}")
    else:
        print("Non ci sono residui")



def recupero_lista_csv_sorgenti(s3_client,source_bucket,prefix_s3,id_simulazione):
    """
    Recuperiamo la lista dei file csv sui quali effettuare l'operazione di IMPORT_DATA

    Args:
        s3_client (botocore.client.S3): connessione ad s3
        source_bucket (string): bucket contenente i file csv sorgenti da importare successivamente tramite l'operazione di IMPORT_DATA
        prefix_s3 (string): prefisso del bucket fino alla cartella contenente i file che verranno successivamente importati tramite l'operazione di IMPORT_DATA 
        id_simulazione (string): identificativo univoco della simulazione sul db

    Returns:
        list: lista dei file csv sui quali effettuare l'operazione di IMPORT_DATA
    """
    objects = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=prefix_s3, Delimiter="/")
    lista_settimane = [cp["Prefix"] for cp in objects.get("CommonPrefixes", [])]
    lista_file_csv = []
    count=1
    for singola_settimana in lista_settimane:
        # siccome stiamo prendendo solo le capacità su provincia, mettiamo un'if per evitare di prendere le capacità dei CAP
        if 'cap_capacities' in singola_settimana:
            continue
        tmp_list = []
        objects = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=singola_settimana)
        for obj in objects.get("Contents", []):
            if obj["Key"][-4:] == '.csv':
                tmp_list.append({'s3_file_key':obj["Key"]})
        lista_file_csv.append({"lista_file_csv_"+str(count):tmp_list})
        # recupero eventuali residui
        if count == 1 and singola_settimana[-3:-1]!='08':
            filekey_csv_residui = recupero_residui(prefix_s3, id_simulazione, singola_settimana)
            lista_file_csv[0]['lista_file_csv_1'].append(filekey_csv_residui)
        count+=1

    # serve per fare in modo di avere sempre 6 settimane. Se ne abbiamo di meno inseriamo le altre vuote
    if len(lista_file_csv)==4:
        lista_file_csv.append({"lista_file_csv_5":[]})
        lista_file_csv.append({"lista_file_csv_6":[]})
    elif len(lista_file_csv)==5:
        lista_file_csv.append({"lista_file_csv_6":[]})
    return lista_file_csv



def lambda_handler(event, context):
    # recupero variabili d'ambiente
    source_bucket = os.environ['source_bucket']
    mese_simulazione = event["mese_simulazione"][:7] # mese_simulazione è del formato YYYY-MM-DD ma a noi interessa solamente YYYY-MM
    tipo_simulazione = event["tipo_simulazione"]
    if tipo_simulazione == 'Automatizzata':
        # recupero parametro id_simulazione
        id_simulazione = event["output_lambda_ConfigurazioneSimulazione"]['Payload']['id_simulazione_automatizzata']
    elif tipo_simulazione == 'Manuale':
        # recupero parametro id_simulazione
        id_simulazione = event['id_simulazione_manuale']
    else:
        raise Exception('Parametro tipo_simulazione non valorizzato')
    # inizializzazione connessione verso s3
    s3_client = boto3.client('s3')
    # recuperiamo il path s3 per prendere i csv delle postalizzazioni
    prefix_s3_settimana_estrazione = recupero_ultima_data_estrazione(source_bucket, s3_client)
    # recuperiamo la lista dei csv delle postalizzazioni
    full_prefix = prefix_s3_settimana_estrazione+mese_simulazione+'/'
    lista_file_csv = recupero_lista_csv_sorgenti(s3_client,source_bucket,full_prefix,id_simulazione)
    
    if len(lista_file_csv) != 0:

        return {'statusCode': 200, 'lista_file_csv': lista_file_csv}
    else:
        raise Exception("Lista file csv vuota")
