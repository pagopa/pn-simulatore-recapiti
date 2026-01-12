import json
from datetime import datetime, timedelta
import boto3



def get_count_residui(lambda_delayer, settimana_processata):
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


def get_first_monday_2_mesi_successivi(data_string):
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
    count_residui_settimana_processata = get_count_residui(lambda_delayer, settimana_processata_RUN_ALGORITHM)
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
            primo_lunedi_2_mesi_successivi = get_first_monday_2_mesi_successivi(lista_settimane_processate[0])
            if primo_lunedi_2_mesi_successivi == lista_settimane_processate[-1]:
                continuare_run_algorithm = 0
            else:
                continuare_run_algorithm = 1
    else:
        continuare_run_algorithm = 1

    

    return {'statusCode': 200, 'lista_settimane_processate':lista_settimane_processate, 'continuare_run_algorithm':continuare_run_algorithm, 'count_residui_ultima_settimana':str(count_residui_settimana_processata)}
