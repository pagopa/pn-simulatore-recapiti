"""
AWS Lambda che si occupa di recuperare la lista dei file da importare tramite l'operazione IMPORT_DATA

Trigger:
    Step function pn-simulatore-recapiti-sf-GestioneSimulazione

Input:
    settimana_processata_RUN_ALGORITHM: ultima settimana processata tramite l'operazione di RUN_ALGORITHM, nel formato yyyy-MM-dd, utile per calcolare la successiva settimana da processare
    mese_simulazione: prima settimana del mese di simulazione, nel formato yyyy-MM-dd
    tipo_simulazione: 'Automatizzata' o 'Manuale'
    start_timestamp_simulazione: timestamp di starting della simulazione
    pianificazione_postalizzazioni: scelta dell'utente che, tramite la webapp, ha selezionato il tipo di pianificazione_postalizzazioni
    postalizzazioni_fuori_commessa: scelta dell'utente che, tramite la webapp, ha selezionato o meno la checkbox delle postalizzazioni fuori commessa

Output:
    lista_file_csv: fornisce alla LambdaInsertMockCapacities la lista dei file assegnati all'IMPORT_DATA per il caricamento
"""
import json
import boto3
from botocore.config import Config
import os
from datetime import datetime, date, timedelta
import math
import urllib3
import io
import csv
import itertools
import codecs


def calcolo_numero_settimana_attuale_nel_mese(start_timestamp_simulazione):
    """
    Funzione che calcola e restituisce il numero della settimana attuale rispetto al mese corrente.

    Args:
        start_timestamp_simulazione (string): timestamp di stard di esecuzione della step function della simulazione, formato "yyyy-MM-dd HH:mm:ss"

    Note:
        - 0=prima settimana, 1=seconda settimana, ...
        - se il primo del mese è lunedì, la prima settimana la segna come 0
        - se il primo del mese non è lunedì, la seconda settimana inizia dal primo lunedì

    Returns:
        int: numero della settimana attuale rispetto al mese corrente
    """
    data_input = datetime.strptime(start_timestamp_simulazione, '%Y-%m-%d %H:%M:%S').date()
    # calcoliamo il primo giorno del mese corrente
    primo_del_mese = data_input.replace(day=1)
    # calcoliamo giorno della settimana del primo del mese (RICORDA: con weekday(), 0=lunedì, 6=domenica)
    offset = primo_del_mese.weekday()
    # calcoliamo numero settimana nel mese
    numero_settimana = (data_input.day + offset - 1) // 7
    return numero_settimana


def recupero_ultima_data_estrazione(bucket_name, mese_simulazione, start_timestamp_simulazione):
    """
    Recuperiamo la data dell'ultimo recupero dati sottoforma di prefisso del bucket s3 di progetto

    Args:
        bucket_name (string): nome del bucket s3 di progetto
        mese_simulazione (string): mese di simulazione, formato "yyyy-MM"
        start_timestamp_simulazione (string): timestamp di stard di esecuzione della step function della simulazione, formato "yyyy-MM-dd HH:mm:ss"

    Returns:
        string: prefisso del bucket fino alla cartella contenente i file che verranno successivamente importati tramite l'operazione di IMPORT_DATA
    """
    target_date = datetime.strptime(start_timestamp_simulazione, '%Y-%m-%d %H:%M:%S').date()
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
    # controlliamo che il file csv non sia vuoto
    if len(file_content) != 0:
        lista_csv_da_importare = []
        # decodifica file csv
        decoded_content = file_content.decode('utf-8')
        # contiamo il numero totale delle righe del csv
        n_rows = decoded_content.count('\n')
        # estraiamo il contenuto del csv
        file_content = csv.reader(io.StringIO(decoded_content), delimiter=';')
        # recuperiamo l'header
        header = next(file_content)
        # il numero massimo di righe per ogni file csv è 10000, ma per essere sicuri mettiamo impostiamo il numero massimo a 9900
        max_rows = 9900
        # dividiamo il csv per far sì che ogni chunk abbia max 9900 righe
        num_chunks=math.ceil(n_rows/max_rows)
        for index in range(num_chunks):
            # ad ogni iterazione prendiamo un chunk da 9900 righe e carichiamo il csv su s3
            chunk = list(itertools.islice(file_content, max_rows))
            s3_file_key = f'{prefix_s3}dati_extra/residui/id_simulazione_{id_simulazione}/{key}_part_{index}.csv'
            # componiamo il file csv
            buffer = io.StringIO()
            writer = csv.writer(buffer, delimiter=';', quoting=csv.QUOTE_ALL)
            writer.writerow(header)
            writer.writerows(chunk)
            # codifica file csv
            csv_file = buffer.getvalue().encode("utf-8")
            # carichiamo il csv su S3
            s3_client = boto3.client('s3') # inizializzazione connessione verso s3
            s3_client.put_object(
                Bucket=os.environ['source_bucket'],
                Key=s3_file_key,
                Body=csv_file,
                ContentType='text/csv'
            )
            lista_csv_da_importare.append({'settimana_import':prima_settimana_simulazione_string,'s3_file_key':s3_file_key,'id_simulazione':id_simulazione})
        return lista_csv_da_importare
    else:
        return []


def gestione_residui(prefix_s3,id_simulazione,prima_settimana_simulazione_string, start_timestamp_simulazione):
    """
    Funzione che gestisce la logica dei residui e ritorna la lista dei file da importare nella prima settimana di run

    Args:
        prefix_s3 (string): prefisso del bucket fino alla cartella dove andremo a depositare la cartella che conterrà il csv dei residui
        id_simulazione (string): identificativo univoco della simulazione sul db
        prima_settimana_simulazione_string (string): data della prima settimana di simulazione, nel formato yyyy-MM-dd
    
    Returns:
        list of dict: lista contenente un dizionario per ogni file csv dei residui che dovrà essere importato nella prima settimana di simulazione
    """
    date_today = datetime.strptime(start_timestamp_simulazione, '%Y-%m-%d %H:%M:%S').date()
    prima_settimana_simulazione = date.fromisoformat(prima_settimana_simulazione_string)
    # controlliamo se vogliamo simulare il mese in cui ci troviamo, un mese passato o il mese successivo
    if (prima_settimana_simulazione.year,prima_settimana_simulazione.month) == (date_today.year,date_today.month):
        # SIMULAZIONE MESE CORRENTE
        if calcolo_numero_settimana_attuale_nel_mese(start_timestamp_simulazione) == 0:
            # caso in cui siamo nella prima settimana, quindi il mese inizia con lunedì oppure il mese inizia a cavallo con la fine del precedente
            delivery_date_residui = prima_settimana_simulazione - timedelta(days=7)
        else:
            # caso in cui siamo dalla seconda settimana in poi
            delivery_date_residui = prima_settimana_simulazione
        # se siamo al lunedì della settimana corrente devo considerare quella precedente perché pianificazione gira il lunedì
        if date_today==delivery_date_residui:
            delivery_date_residui = delivery_date_residui - timedelta(days=7)
    elif (prima_settimana_simulazione.year,prima_settimana_simulazione.month) > (date_today.year,date_today.month):
        # SIMULAZIONE MESE FUTURO CUT-OFF (ricorda: da requisito, recuperiamo i residui solo se simuliamo il mese successivo)
        if prima_settimana_simulazione.year == date_today.year and ((prima_settimana_simulazione.month - date_today.month) == 1):
            delivery_date_residui = date_today - timedelta(days=date_today.weekday())
            # se siamo al lunedì della settimana corrente devo considerare quella precedente perché pianificazione gira il lunedì
            if date_today==delivery_date_residui:
                delivery_date_residui = delivery_date_residui - timedelta(days=7)   
        else:
            delivery_date_residui = None
    else:
        # SIMULAZIONE MESE PASSATO
        delivery_date_residui = prima_settimana_simulazione
    # recuperiamo i residui per poi fare import data sulla prima settimana di simulazione
    if delivery_date_residui:
        print(f'La delivery date per il recupero dei residui è: {delivery_date_residui}')
        lista_file_residui = recupero_residui(str(delivery_date_residui),prefix_s3,id_simulazione,prima_settimana_simulazione_string)
        if len(lista_file_residui) != 0:
            print("Per questa simulazione ci sono residui!")
        else:
            print("Per questa simulazione non sono stati trovati residui da recuperare!")
        return lista_file_residui
    else:
        print("Per questa simulazione non verranno recuperati residui!")
        return []


def popolamento_lista_file_csv(s3_client,source_bucket,lista_settimane,id_simulazione):
    """
        Popoliamo la lista dei file csv postalizzazioni (default o mock) sui quali effettuare l'operazione di IMPORT_DATA
    
        Args:
            s3_client (botocore.client.S3): connessione ad s3
            source_bucket (string): bucket contenente i file csv sorgenti da importare successivamente tramite l'operazione di IMPORT_DATA
            lista_settimane (list): lista settimane di simulazione contenenti postalizzazioni (default o mock), formato "yyyy-MM-dd"
            id_simulazione (string): identificativo univoco della simulazione sul db
    
        Returns:
            list: lista dei file csv postalizzazioni (default o mock) sui quali effettuare l'operazione di IMPORT_DATA
    """
    lista_file_da_appendere = []
    for singola_settimana in lista_settimane:
        objects = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=singola_settimana)
        for obj in objects.get("Contents", []):
            if obj["Key"][-4:] == '.csv':
                # nota: singola_settimana ha il formato settimana_import avrà il formato avrà il formato 'yyyy-MM-dd_settimana_esecuzione'
                lista_file_da_appendere.append({'settimana_import':singola_settimana.split('/')[-2],'s3_file_key':obj["Key"],'id_simulazione':id_simulazione})
    return lista_file_da_appendere


def recupero_lista_csv_sorgenti(source_bucket,prefix_s3,id_simulazione,prima_settimana_simulazione,start_timestamp_simulazione,pianificazione_postalizzazioni,postalizzazioni_fuori_commessa):
    """
    Recuperiamo la lista dei file csv sui quali effettuare l'operazione di IMPORT_DATA

    Args:
        source_bucket (string): bucket contenente i file csv sorgenti da importare successivamente tramite l'operazione di IMPORT_DATA
        prefix_s3 (string): prefisso del bucket fino alla cartella contenente i file che verranno successivamente importati tramite l'operazione di IMPORT_DATA 
        id_simulazione (string): identificativo univoco della simulazione sul db
        prima_settimana_simulazione (string): prima settimana di simulazione, formato "yyyy-MM-dd"
        pianificazione_postalizzazioni (string): scelta dell'utente che, tramite la webapp, ha selezionato il tipo di pianificazione_postalizzazioni
        postalizzazioni_fuori_commessa: scelta dell'utente che, tramite la webapp, ha selezionato o meno la checkbox delle postalizzazioni fuori commessa

    Returns:
        list: lista dei file csv sui quali effettuare l'operazione di IMPORT_DATA
    """
    # inizializzazione connessione verso s3
    s3_client = boto3.client('s3')
    lista_file_csv = []
    # RECUPERO POSTALIZZAZIONI DEFAULT
    if (pianificazione_postalizzazioni != 'Utilizza solo le commesse di mock'):
        # recuperiamo la lista delle cartelle di interesse sulla cartella di destinazione s3
        objects = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=prefix_s3, Delimiter="/")
        lista_settimane = [cp["Prefix"] for cp in objects.get("CommonPrefixes", [])]
        # siccome stiamo prendendo solo le capacità su provincia, mettiamo un'if per evitare di prendere dati_extra       
        lista_settimane = [x for x in lista_settimane if '/dati_extra/' not in x]
        lista_file_csv.extend(popolamento_lista_file_csv(s3_client,source_bucket,lista_settimane,id_simulazione))
    # RECUPERO POSTALIZZAZIONI MOCK
    if (pianificazione_postalizzazioni == 'Utilizza le commesse di default e le commesse di mock' or pianificazione_postalizzazioni == 'Utilizza solo le commesse di mock' or postalizzazioni_fuori_commessa=='True'):
        # recuperiamo la lista delle cartelle di interesse sulla cartella di destinazione s3
        objects = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=prefix_s3+f'dati_extra/postalizzazioni_mock/ID_{id_simulazione}/', Delimiter="/")
        lista_settimane = [cp["Prefix"] for cp in objects.get("CommonPrefixes", [])]
        lista_file_csv.extend(popolamento_lista_file_csv(s3_client,source_bucket,lista_settimane,id_simulazione))  
    # recupero residui
    lista_file_csv.extend(gestione_residui(prefix_s3, id_simulazione, prima_settimana_simulazione, start_timestamp_simulazione))
    return lista_file_csv



def lambda_handler(event, context):
    # recupero variabili d'ambiente
    source_bucket = os.environ['source_bucket']
    prima_settimana_simulazione = event["mese_simulazione"]
    mese_simulazione = prima_settimana_simulazione[:7] # mese_simulazione che recuperiamo dalla step function è nel formato yyyy-MM-dd ma a noi interessa solamente yyyy-MM
    tipo_simulazione = event["tipo_simulazione"]
    start_timestamp_simulazione = event["output_lambda_ConfigurazioneSimulazione"]['Payload']['start_timestamp_simulazione']
    pianificazione_postalizzazioni = event["output_lambda_ConfigurazioneSimulazione"]['Payload']["pianificazione_postalizzazioni"]
    postalizzazioni_fuori_commessa = event["output_lambda_ConfigurazioneSimulazione"]['Payload']["postalizzazioni_fuori_commessa"]
    if tipo_simulazione == 'Automatizzata':
        # recupero parametro id_simulazione
        id_simulazione = event["output_lambda_ConfigurazioneSimulazione"]['Payload']['id_simulazione_automatizzata']
    elif tipo_simulazione == 'Manuale':
        # recupero parametro id_simulazione
        id_simulazione = event['id_simulazione_manuale']
    else:
        raise Exception('Parametro tipo_simulazione non valorizzato')
    # recuperiamo il path s3 per prendere i csv delle postalizzazioni
    full_prefix = recupero_ultima_data_estrazione(source_bucket, mese_simulazione, start_timestamp_simulazione)
    # recuperiamo la lista dei csv delle postalizzazioni
    lista_file_csv = recupero_lista_csv_sorgenti(source_bucket,full_prefix,id_simulazione,prima_settimana_simulazione,start_timestamp_simulazione,pianificazione_postalizzazioni,postalizzazioni_fuori_commessa)
    # generiamo un'eccezione se la lista_file_csv è vuota
    if len(lista_file_csv) != 0:
        return {'statusCode': 200, 'lista_file_csv': lista_file_csv}
    else:
        raise Exception("Lista file csv vuota, errore durante il recupero delle postalizzazioni!")
