import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def lambda_handler(event, context):
    try:
        # se al lancio della step function è stata specificata la date_simulazione
        date_simulazione = event["date_simulazione"]
        if len(date_simulazione) != 0:
            return {"date_simulazione":date_simulazione}
    except:
        # datetime now
        datetime_now = datetime.now(ZoneInfo("Europe/Rome"))
        giorno = datetime_now.day
        mese = datetime_now.month
        anno = datetime_now.year

        # REQUISITO: dopo il 16 del mese corrente bisogna processare il mese successivo
        
        if giorno > 16:
            # aumentiamo il mese di 1
            if mese == 12:
                anno = anno + 1
                mese = 1
            else:
                mese = mese + 1
        # primo giorno del mese
        first = datetime(anno, mese, 1)
        # giorno della settimana (lunedì=0, ... domenica=6)
        weekday = first.weekday()
        # calcoliamo quanto manca al primo lunedì
        giorni_fino_lunedi = (7 - weekday) % 7
        # recuperiamo il primo lunedì
        prima_settimana_da_processare = first + timedelta(days=giorni_fino_lunedi)
        # se il primo lunedì del mese è 1, prendiamo l'8 come prima settimana da processare
        if prima_settimana_da_processare.day == 1:
            prima_settimana_da_processare = prima_settimana_da_processare + timedelta(days=7)

        # formattiamo la data come "yyyy-mm-dd"
        date_simulazione = [
            {
                "mese_simulazione": str(prima_settimana_da_processare.date())
            }
        ]

        return {"date_simulazione":date_simulazione}
