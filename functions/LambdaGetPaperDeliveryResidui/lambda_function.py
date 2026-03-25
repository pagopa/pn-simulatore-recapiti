"""
AWS Lambda che gestisce il ciclo di RUN_ALGORITHM andando ad aggiornare la lista delle settimane processate, calcolando se continuare a lanciare o meno la RUN_ALGORITHM

Trigger:
    Step function pn-simulatore-recapiti-sf-GestioneSimulazione

Input:
    settimana_processata_RUN_ALGORITHM: indica la settimana di simulazione processata dalla RUN_ALGORITHM nello step precedente
    lista_settimane_processate: lista delle settimane già processate, da aggiornare aggiungendo la settimana appena processata dalla RUN_ALGORITHM

Output:
    lista_settimane_processate: lista settimane già processate
    continuare_run_algorithm: 1 per continuare a lanciare la RUN_ALGORITHM, 0 per fermarsi
    count_residui_ultima_settimana: conteggio dei residui con un max si 1000. Questo conteggio serve per capire se continuare a lanciare la RUN_ALGORITHM (se ci troviamo dopo la prima settimana del mese successivo) o fermarsi
"""
import json
from datetime import datetime, timedelta
import boto3

def conta_residui(lambda_delayer, settimana_processata):
    """
    Recupera i residui tramite l'operazione GET_PAPER_DELIVERY. Non ci importa capire quanti residui sono ma solamente se ci sono o meno una chiamata per capire se ci sono o meno

    Args:
        lambda_delayer (botocore.client.Lambda): connessione alla lambda
        settimana_processata (string): data nel formato yyyy-mm-dd corrispondente al lunedì della settimana processata, da dare in input all'operazione GET_PAPER_DELIVERY

    Returns:
        int: conteggio residui
    """
    # GET_PAPER_DELIVERY - testDelayerLambda
    payload_lambda={
        "operationType": "GET_PAPER_DELIVERY",
        "parameters": ["pn-DelayerPaperDeliveryMock", settimana_processata, "EVALUATE_SENDER_LIMIT"]
    }
    response_lambda=lambda_delayer.invoke(FunctionName='pn-testDelayerLambda',Payload=json.dumps(payload_lambda))
    read_response = response_lambda['Payload'].read()
    string_response = read_response.decode('utf-8')
    response_dict = json.loads(string_response)
    if response_dict['statusCode'] not in (200, 201, 204):
        raise Exception(response_dict['body'])
    response_dict_body = json.loads(response_dict['body'])
    count_items = len(response_dict_body['items'])
    return count_items


def calcolo_primo_lunedi_due_mesi_successivi(data_string):
    """
    Calcoliamo il primo lunedì di due mesi successivi rispetto alla data fornita in input

    Args:
        data_string (string): primo lunedì processato dalla RUN_ALGORITHM nel formato yyyy-mm-dd

    Returns:
        string: primo lunedì di due mesi successivi nel formato yyyy-mm-dd
    """
    # from string to datetime
    d = datetime.strptime(data_string, "%Y-%m-%d")
    # aumentiamo il mese di 2
    if d.month == 11:
        anno = d.year + 1
        mese = 1
    elif d.month == 12:
        anno = d.year + 1
        mese = 2
    else:
        anno = d.year
        mese = d.month + 2
    # primo giorno di 2 mesi successivi
    first = datetime(anno, mese, 1)
    # giorno della settimana (lunedì=0, ... domenica=6)
    weekday = first.weekday()
    # calcoliamo quanto manca al primo lunedì
    giorni_fino_lunedi = (7 - weekday) % 7
    # recuperiamo il primo lunedì
    primo_lunedi = first + timedelta(days=giorni_fino_lunedi)
    return primo_lunedi.strftime("%Y-%m-%d")


def lambda_handler(event, context):
    lambda_delayer = boto3.client('lambda')
    # recupero parametri e variabili d'ambiente
    settimana_processata_RUN_ALGORITHM = event['output_lambda_RUNALGORITHM']['Payload']['settimana_processata_RUN_ALGORITHM']

    # se siamo nelle iterazioni successive alla prima recuperiamo la lista 'lista_settimane_processate' dai parametri, altrimenti la creiamo
    try:
        lista_settimane_processate = event["output_lambda_CountResidui"]['Payload']['lista_settimane_processate']
    except:
        lista_settimane_processate = []
    
    # recuperiamo i residui
    count_residui_settimana_processata = conta_residui(lambda_delayer, settimana_processata_RUN_ALGORITHM)
    if count_residui_settimana_processata < 1000:
        print(f'Dopo aver processato la settimana {settimana_processata_RUN_ALGORITHM} i residui sono: {count_residui_settimana_processata}')
    else:
        print(f'Dopo aver processato la settimana {settimana_processata_RUN_ALGORITHM} i residui sono >= {count_residui_settimana_processata}')

    # aggiungiamo la settimana processata alla lista_settimane_processate
    lista_settimane_processate.append(settimana_processata_RUN_ALGORITHM)

    # se ci sono almeno 5 settimane vuol dire che abbiamo appena runnato l'ultima settimana del mese oppure la prima settimana del mese successivo
    if len(lista_settimane_processate) >= 5:
        # controllo se il mese della prima settimana runnata è diverso dal mese dell'ultima settimana runnata. Se sono diversi vuol dire che ho già runnato almeno la prima settimana del mese successivo
        if count_residui_settimana_processata==0 and datetime.strptime(lista_settimane_processate[0], "%Y-%m-%d").month != datetime.strptime(lista_settimane_processate[-1], "%Y-%m-%d").month:
            continuare_run_algorithm = 0
        else:
            primo_lunedi_2_mesi_successivi = calcolo_primo_lunedi_due_mesi_successivi(lista_settimane_processate[0])
            if primo_lunedi_2_mesi_successivi == lista_settimane_processate[-1]:
                continuare_run_algorithm = 0
            else:
                continuare_run_algorithm = 1
    else:
        continuare_run_algorithm = 1

    

    return {'statusCode': 200, 'lista_settimane_processate':lista_settimane_processate, 'continuare_run_algorithm':continuare_run_algorithm, 'count_residui_ultima_settimana':str(count_residui_settimana_processata)}
