import json
import boto3
import os
from datetime import date, timedelta


def get_ultima_data_estrazione(bucket_name, s3_client):
    target_date = date.today()

    for _ in range(30):  # limite di sicurezza a 30 gg
        prefix = target_date.strftime("%Y/%m/%d/")
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix='input/'+prefix,
            MaxKeys=1
        )
        # se la cartella esiste, ritorno la data
        if 'Contents' in response:
            return prefix
        # altrimenti vado al giorno precedente
        target_date -= timedelta(days=1)
    # se non viene trovata alcuna cartella corrispondente
    raise Exception("Nessuna folder input/yyyy/mm/dd_di_estrazione su S3")


def get_lista_csv_source(s3_client,source_bucket,prefix_s3):
    objects = s3_client.list_objects_v2(Bucket=source_bucket, Prefix='input/'+prefix_s3, Delimiter="/")
    lista_settimane = [cp["Prefix"] for cp in objects.get("CommonPrefixes", [])]
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

    # serve per fare in modo di avere sempre 5 settimane (se ne sono 4, la quinta la inseriamo vuota)
    if len(lista_settimane)==4:
        lista_file_csv.append({"lista_file_csv_"+str(count):[]})
    return lista_file_csv



def lambda_handler(event, context):
    # recupero variabili d'ambiente
    source_bucket = os.environ['source_bucket']
    mese_simulazione = event["mese_simulazione"][:7] # mese_simulazione è del formato yyyy-mm-dd ma a noi interessa solamente yyyy-mm
    # inizializzazione connessione verso s3
    s3_client = boto3.client('s3')
    # recuperiamo il path s3 per prendere i csv delle postalizzazioni
    prefix_s3_settimana_estrazione = get_ultima_data_estrazione(source_bucket, s3_client)
    # recuperiamo la lista dei csv delle postalizzazioni
    full_prefix = prefix_s3_settimana_estrazione+mese_simulazione+'/'
    lista_file_csv = get_lista_csv_source(s3_client,source_bucket,full_prefix)
    
    if len(lista_file_csv) != 0:

        return {'statusCode': 200, 'lista_file_csv': lista_file_csv}
    else:
        raise Exception("Lista file csv vuota")
