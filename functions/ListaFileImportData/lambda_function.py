"""
AWS Lambda che si occupa di recuperare la lista dei file da importare tramite l'operazione IMPORT_DATA

Trigger:
    Step function pn-simulatore-recapiti-sf-GestioneSimulazione

Input:
    settimana_processata_RUN_ALGORITHM: ultima settimana processata tramite l'operazione di RUN_ALGORITHM, nel formato yyyy-MM-dd, utile per calcolare la successiva settimana da processare
    mese_simulazione: prima settimana del mese di simulazione, nel formato yyyy-MM-dd
    tipo_simulazione: 'Automatizzata' o 'Manuale'

Output:
    lista_file_csv: fornisce alla LambdaInsertMockCapacities la lista dei file assegnati all'IMPORT_DATA per il caricamento
"""
import json
import boto3
from botocore.config import Config
import os
from datetime import date, timedelta
import math
import urllib3
import io
import csv
import itertools
import codecs


def recupero_ultima_data_estrazione(bucket_name, mese_simulazione):
    """
    Recuperiamo la data dell'ultimo recupero dati sottoforma di prefisso del bucket s3 di progetto

    Args:
        bucket_name (string): nome del bucket s3 di progetto
        mese_simulazione (string): mese di simulazione, formato "yyyy-MM"

    Returns:
        string: prefisso del bucket fino alla cartella contenente i file che verranno successivamente importati tramite l'operazione di IMPORT_DATA
    """
    target_date = date.today()
    # inizializzazione connessione verso s3
    s3_client = boto3.client('s3')
    for _ in range(120):  # limite di sicurezza a 120 gg
        prefix = target_date.strftime("%Y/%m/%d/")
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix='input/'+prefix+mese_simulazione+'/',
            MaxKeys=1
        )
        # se la cartella esiste, ritorno la key fino alla data dell'ultimo recupero dati
        if 'Contents' in response:
            return 'input/'+prefix+mese_simulazione+'/'
        # altrimenti vado al giorno precedente
        target_date -= timedelta(days=1)
    # se non viene trovata alcuna cartella corrispondente
    raise Exception("Nessuna folder input/yyyy/MM/dd_di_estrazione/yyyy_MM_simulazione su S3 creata negli ultimi 120 gg")

def upload_chunk_su_s3(prefix_s3, id_simulazione, key, index, header, chunk):
    """
    Carichiamo il chunk del csv dei residui su s3

    Args:
        prefix_s3 (string): prefisso del bucket fino alla cartella dove andremo a depositare la cartella che conterrà il csv dei residui
        id_simulazione (string): identificativo univoco della simulazione sul db
        key (string): nome del file csv originale dei residui
        index (int): indice incrementale per distinguere le partizioni
        header (string): intestazione del file csv
        chunk (list): lista di righe da inserire nel csv

    Returns:
        string: file_key del file caricato su s3
    """
    s3_file_key = f'{prefix_s3}residui/id_simulazione_{id_simulazione}/{key}_part_{index}.csv'
    # componiamo il file csv
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=';', quoting=csv.QUOTE_ALL)
    writer.writerow(header)
    writer.writerows(chunk)
    # carichiamo il csv su S3
    s3_client = boto3.client('s3')
    s3_client.put_object(
        Bucket=os.environ['source_bucket'],
        Key=s3_file_key,
        Body=buffer.getvalue().encode('utf-8'), # codifica file csv
        ContentType='text/csv'
    )
    return s3_file_key


def recupero_residui(deliveryDate,prefix_s3,id_simulazione,prima_settimana_simulazione_string):
    """
    Funzione che recupera i residui attraverso la lambda 'GET_RESIDUAL_PAPERS', salva il/i csv dei residui su s3 (split se vi sono più di 10k righe) e ritorna la lista del/dei csv caricato/i su s3

    Args:
        deliveryDate (string): indica la settimana per il recupero dei residui, nel formato 'yyyy-MM-dd'
        prefix_s3 (string): prefisso del bucket fino alla cartella dove andremo a depositare la cartella che conterrà il csv dei residui
        id_simulazione (string): identificativo univoco della simulazione sul db
        prima_settimana_simulazione_string (string): data della prima settimana di simulazione, nel formato yyyy-MM-dd
    
    Returns:
        list: lista contenente un dizionario per ogni file csv dei residui che dovrà essere importato nella settimana target di simulazione
    """
    # inizializzazione urllib3
    http = urllib3.PoolManager()
    # chiamiamo la GET_RESIDUAL_PAPERS dando in input la deliveryDate
    config = Config(read_timeout=900) # allungato a 15 minuti
    lambda_delayer = boto3.client('lambda',config=config)
    payload_lambda={
        "operationType": "GET_RESIDUAL_PAPERS",
        "parameters": ["pn_delayer_paper_delivery_json_view", deliveryDate]
    }
    # gestione risposta GET_RESIDUAL_PAPERS
    response_lambda=lambda_delayer.invoke(FunctionName='pn-testDelayerLambda',Payload=json.dumps(payload_lambda))
    read_response = response_lambda['Payload'].read()
    string_response = read_response.decode('utf-8')
    response_dict = json.loads(string_response)
    if response_dict['statusCode'] != 200:
        raise Exception(f"Errore durante la GET_RESIDUAL_PAPERS: {response_dict}")
    downloadUrl = json.loads(response_dict['body'])['downloadUrl']
    key = json.loads(response_dict['body'])['key'].split('/')[-1][:-4]
    # download file dal presigned url
    response = http.request('GET', downloadUrl, preload_content=False)
    # check stato risposta
    if response.status != 200:
        raise Exception(f"Errore durante il download dei residui, statusCode: {response.status}")
    # il numero massimo di righe per ogni file csv è 10000, ma per essere sicuri mettiamo impostiamo il numero massimo a 9900
    max_rows = 9900
    lista_csv_da_importare = []
    # decodifica in streaming, senza portarsi tutto il file in memoria
    reader = csv.reader(codecs.iterdecode(response, 'utf-8'), delimiter=';')
    # controlliamo che il csv non sia vuoto
    try:
        # recuperiamo l'header
        header = next(reader)
    except StopIteration:
        # il csv è completamente vuoto ed è senza header
        header = None
    if header is not None:
        index = 0
        chunk = []
        for row in reader:
            # ad ogni iterazione prendiamo un chunk da 9900 righe e carichiamo il csv su s3
            chunk.append(row)
            if len(chunk) >= max_rows:
                s3_file_key = upload_chunk_su_s3(prefix_s3, id_simulazione, key, index, header, chunk)
                lista_csv_da_importare.append({'settimana_import': prima_settimana_simulazione_string, 's3_file_key': s3_file_key})
                chunk = []
                index += 1
        # ultimo chunk
        if chunk:
            s3_file_key = upload_chunk_su_s3(prefix_s3, id_simulazione, key, index, header, chunk)
            lista_csv_da_importare.append({'settimana_import': prima_settimana_simulazione_string, 's3_file_key': s3_file_key})                            
    # chiudiamo la connessione
    response.release_conn()

    return lista_csv_da_importare


def recupero_lista_csv_sorgenti(source_bucket,prefix_s3,id_simulazione,prima_settimana_simulazione):
    """
    Recuperiamo la lista dei file csv sui quali effettuare l'operazione di IMPORT_DATA

    Args:
        source_bucket (string): bucket contenente i file csv sorgenti da importare successivamente tramite l'operazione di IMPORT_DATA
        prefix_s3 (string): prefisso del bucket fino alla cartella contenente i file che verranno successivamente importati tramite l'operazione di IMPORT_DATA 
        id_simulazione (string): identificativo univoco della simulazione sul db
        prima_settimana_simulazione (string): prima settimana di simulazione, formato "yyyy-MM-dd"

    Returns:
        list: lista dei file csv sui quali effettuare l'operazione di IMPORT_DATA
    """
    # inizializzazione connessione verso s3
    s3_client = boto3.client('s3')
    objects = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=prefix_s3, Delimiter="/")
    lista_settimane = [cp["Prefix"] for cp in objects.get("CommonPrefixes", [])]
    # siccome stiamo prendendo solo le capacità su provincia, mettiamo un'if per evitare di prendere le capacità dei CAP            
    lista_settimane = [x for x in lista_settimane if 'cap_capacities' not in x]
    lista_file_csv = []
    count=1
    for singola_settimana in lista_settimane:
        tmp_list = []
        objects = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=singola_settimana)
        for obj in objects.get("Contents", []):
            if obj["Key"][-4:] == '.csv':
                tmp_list.append({'s3_file_key':obj["Key"]})
        lista_file_csv.append({"lista_file_csv_"+str(count):tmp_list})
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
    prima_settimana_simulazione = event["mese_simulazione"]
    mese_simulazione = prima_settimana_simulazione[:7] # mese_simulazione che recuperiamo dalla step function è nel formato yyyy-MM-dd ma a noi interessa solamente yyyy-MM
    tipo_simulazione = event["tipo_simulazione"]
    if tipo_simulazione == 'Automatizzata':
        # recupero parametro id_simulazione
        id_simulazione = event["output_lambda_ConfigurazioneSimulazione"]['Payload']['id_simulazione_automatizzata']
    elif tipo_simulazione == 'Manuale':
        # recupero parametro id_simulazione
        id_simulazione = event['id_simulazione_manuale']
    else:
        raise Exception('Parametro tipo_simulazione non valorizzato')
    # recuperiamo il path s3 per prendere i csv delle postalizzazioni
    full_prefix = recupero_ultima_data_estrazione(source_bucket, mese_simulazione)
    # recuperiamo la lista dei csv delle postalizzazioni
    lista_file_csv = recupero_lista_csv_sorgenti(source_bucket,full_prefix,id_simulazione,prima_settimana_simulazione)
    
    if len(lista_file_csv) != 0:

        return {'statusCode': 200, 'lista_file_csv': lista_file_csv}
    else:
        raise Exception("Lista file csv vuota")
