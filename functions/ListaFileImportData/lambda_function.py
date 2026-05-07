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

def calcolo_ultimo_lunedi_mese_precedente(data_str):
    """
    Funzione che prende in input una data stringa nel formato 'yyyy-MM-dd' e restituisce l'ultimo lunedì del mese precedente

    Args:
        data_str (string): indica la data, nel formato 'yyyy-MM-dd', a partire dalla quale vogliamo calcolare l'ultimo lunedì del mese precedente

    Returns:
        str: data (formato 'yyyy-MM-dd') dell'ultima settimana del mese precedente rispetto alla data di input
    """
    # cast da string a date
    d = date.fromisoformat(data_str)
    # calcoliamo il primo giorno del mese corrente
    primo_del_mese = d.replace(day=1)
    # calcoliamo l'ultimo giorno del mese precedente
    ultimo_del_mese_prec = primo_del_mese - timedelta(days=1)
    # torniamo indietro fino al lunedì (RICORDA: con weekday(), 0=lunedì, 6=domenica)
    giorni_indietro = ultimo_del_mese_prec.weekday()  
    ultimo_lunedi = ultimo_del_mese_prec - timedelta(days=giorni_indietro)
    return str(ultimo_lunedi)

def calcolo_numero_settimana_attuale_nel_mese():
    """
    Funzione che calcola e restituisce il numero della settimana attuale rispetto al mese corrente

    Returns:
        int: numero della settimana attuale rispetto al mese corrente
    """
    # recuperiamo la data odierna
    d = date.today()
    # calcoliamo il primo giorno del mese corrente
    primo_del_mese = d.replace(day=1)
    # calcoliamo giorno della settimana del primo del mese (RICORDA: con weekday(), 0=lunedì, 6=domenica)
    offset = primo_del_mese.weekday()
    # calcoliamo numero settimana nel mese
    numero_settiamna = (d.day + offset - 1) // 7
    return numero_settiamna


def recupero_ultima_data_estrazione(bucket_name):
    """
    Recuperiamo la data dell'ultimo recupero dati sottoforma di prefisso del bucket s3 di progetto

    Args:
        bucket_name (string): nome del bucket s3 di progetto

    Returns:
        string: prefisso del bucket fino alla cartella contenente i file che verranno successivamente importati tramite l'operazione di IMPORT_DATA
    """
    target_date = date.today()
    # inizializzazione connessione verso s3
    s3_client = boto3.client('s3')
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
    raise Exception("Nessuna folder input/yyyy/MM/dd_di_estrazione su S3 creata negli ultimi 30 gg")

def recupero_residui(import_week,deliveryDate,prefix_s3,id_simulazione):
    """
    Funzione che recupera i residui attraverso la lambda 'GET_RESIDUAL_PAPERS', salva il/i csv dei residui su s3 (split se vi sono più di 10k righe) e ritorna la lista del/dei csv caricato/i su s3

    Args:
        import_week (string): indica la settimana di simulazione sulla quale andare a fare import dei residui recuperati ('week1' o 'week2')
        deliveryDate (string): indica la settimana per il recupero dei residui, nel formato 'yyyy-MM-dd'
        prefix_s3 (string): prefisso del bucket fino alla cartella dove andremo a depositare la cartella che conterrà il csv dei residui
        id_simulazione (string): identificativo univoco della simulazione sul db
    
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
    # dalla resposta alla GET_RESIDUAL_PAPERS recuperiamo link (per il download contenente il csv dei residui) e nome del file generato
    downloadUrl = json.loads(response_dict['body'])['downloadUrl']
    key = json.loads(response_dict['body'])['key'].split('/')[-1][:-4] # rimuoviamo il .csv dal nome del file
    # download file dal presigned url
    response = http.request('GET', downloadUrl, preload_content=False)
    # check stato risposta
    if response.status != 200:
        raise Exception(f"Errore durante il download dei residui, statusCode: {response.status}")
    # leggiamo il contenuto del file
    file_content = response.data
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
        file_content = csv.reader(io.StringIO(decoded_content))
        # recuperiamo l'header
        header = next(file_content)
        # il numero massimo di righe per ogni file csv è 10000, ma per essere sicuri mettiamo impostiamo il numero massimo a 9900
        max_rows = 9900
        # dividiamo il csv per far sì che ogni chunk abbia max 9900 righe
        num_chunks=math.ceil(n_rows/max_rows)
        for index in range(num_chunks):
            # ad ogni iterazione prendiamo un chunk da 9900 righe e carichiamo il csv su s3
            chunk = list(itertools.islice(file_content, max_rows))
            s3_file_key = f'{prefix_s3}residui_id_{id_simulazione}/{import_week}/{key}_part_{index}.csv'
            # componiamo il file csv
            buffer = io.StringIO()
            writer = csv.writer(buffer, delimiter=';')
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
            lista_csv_da_importare.append({'s3_file_key':s3_file_key})
        return lista_csv_da_importare
    else:
        print(f"Non ci sono residui per la {import_week}, file {key}")
        return []


def gestione_residui(prefix_s3,id_simulazione,prima_settimana_simulazione_string):
    """
    Funzione che gestisce la logica dei residui

    Args:
        prefix_s3 (string): prefisso del bucket fino alla cartella dove andremo a depositare la cartella che conterrà il csv dei residui
        id_simulazione (string): identificativo univoco della simulazione sul db
        prima_settimana_simulazione_string (string): data della prima settimana di simulazione, nel formato yyyy-MM-dd
    
    Returns:
        list of dict: lista contenente un dizionario per ogni file csv dei residui che dovrà essere importato nella prima settimana di simulazione
        list of dict: lista contenente un dizionario per ogni file csv dei residui che dovrà essere importato nella seconda settimana di simulazione
    """
    prima_settimana_simulazione = date.fromisoformat(prima_settimana_simulazione_string)
    # recuperiamo l'ultima settimana del mese precedente rispetto al mese da simulare
    ultima_settimana_mese_precedente = calcolo_ultimo_lunedi_mese_precedente(prima_settimana_simulazione_string)
    # controlliamo se vogliamo simulare mesi successivi o meno rispetto a quello in cui ci troviamo
    if (date.today().month + 1) >= prima_settimana_simulazione.month:
        if date.today().month == prima_settimana_simulazione.month and prima_settimana_simulazione.day == 8 and (calcolo_numero_settimana_attuale_nel_mese() == 1 or calcolo_numero_settimana_attuale_nel_mese() == 2):
            # caso: il mese inizia con lunedì 1, siamo nella settimana dell'8 o del 15 e vogliamo simulare il mese attuale 
            delivery_date_residui = prima_settimana_simulazione - timedelta(days=7)
        else:
            delivery_date_residui = ultima_settimana_mese_precedente
    else:
        delivery_date_residui = date.today() - timedelta(days=date.today().weekday()) # RICORDA: 0=lunedì, 6=domenica
    # recuperiamo i residui per poi fare import data sulla prima settimana di simulazione
    residui_week_1 = recupero_residui('week1',delivery_date_residui,prefix_s3,id_simulazione)
    # controlliamo se devo recuperare i residui per poi fare import data sulla seconda settimana di simulazione
    if prima_settimana_simulazione.month == date.today().month and calcolo_numero_settimana_attuale_nel_mese() == 2:
        # caso: siamo nella settimana del secondo lunedì del mese (o nella terza se il mese inizia con lunedì 1) e vogliamo simulare il mese corrente. Recupero i residui della prima settimana e faccio import sulla seconda settimana
        residui_week_2 = recupero_residui('week2',prima_settimana_simulazione,prefix_s3,id_simulazione)
    else:
        residui_week_2 = []
    return residui_week_1, residui_week_2


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
    # siccome stiamo prendendo solo le capacità su provincia, mettiamo un'if per evitare di prendere le capacità dei CAP o i residui            
    lista_settimane = [x for x in lista_settimane if 'cap_capacities' not in x or 'residui_id_' not in x]
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
    # recupero eventuali residui
    residui_week_1, residui_week_2 = gestione_residui(prefix_s3, id_simulazione, prima_settimana_simulazione)
    lista_file_csv[0]['lista_file_csv_1'].extend(residui_week_1)
    lista_file_csv[1]['lista_file_csv_2'].extend(residui_week_2)

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
    prefix_s3_settimana_estrazione = recupero_ultima_data_estrazione(source_bucket)
    # recuperiamo la lista dei csv delle postalizzazioni
    full_prefix = prefix_s3_settimana_estrazione+mese_simulazione+'/'
    lista_file_csv = recupero_lista_csv_sorgenti(source_bucket,full_prefix,id_simulazione,prima_settimana_simulazione)
    
    if len(lista_file_csv) != 0:

        return {'statusCode': 200, 'lista_file_csv': lista_file_csv}
    else:
        raise Exception("Lista file csv vuota")
