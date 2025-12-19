import json
import boto3
import pg8000
import os

def get_db_credentials():
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=os.environ['secretsManager_SecretId'])
    response_SecretString = json.loads(response['SecretString'])
    return response_SecretString


def get_connection():
    creds = get_db_credentials()
    conn = pg8000.connect(
        host=os.environ['DB_HOST'],
        database=os.environ['DB_NAME'],
        user=creds["username"],
        password=creds["password"],
        port=os.environ['DB_PORT']
    )
    return conn

def remove_tmp_folder_on_s3():
    source_bucket = os.environ['source_bucket']
    s3_client = boto3.client('s3')
    objects = s3_client.list_objects_v2(Bucket=source_bucket, Prefix='StepFunction_ListaFileImportData')
    for obj in objects.get("Contents", []):
        s3_client.delete_object(Bucket=source_bucket, Key=obj["Key"])


def lambda_handler(event, context):
    # recuperiamo l'id_simulazione_automatizzata (se automatizzata dall'output della lambda pn-simulatore-recapiti-CreaSimulazioneAutomatizzataDB, se manuale dai parametri d'ambiente della step function)
    if event['tipo_simulazione'] == 'Automatizzata':
        id_simulazione = event["output_lambda_ConfigurazioneSimulazione"]['Payload']['id_simulazione_automatizzata']
    elif event['tipo_simulazione'] == 'Manuale':
        id_simulazione = event['id_simulazione_manuale']
    else:
        raise Exception('Nessun id_simulazione valorizzato')
    
    # in base all'output dei task precedenti capiamo se lo stato è 'Lavorata' o 'Non completata' -> la prima condizione ci fa capire che abbiamo effettuato tutti i RUN_ALGORITHM con successo, la seconda che le postalizzazioni sono state importate con successo tramite IMPORT_DATA 
    if 'output_lambda_RecuperoCapacitaDiProduzione' in event and len(event['output_lambda_ListaFileImportData']['Payload']['lista_file_csv'])!=0 and event['output_lambda_INSERTMOCKCAPACITIES']['Payload']['errori_presenti']==0:
        stato_simulazione = 'Lavorata'
    else:
        stato_simulazione = 'Non completata'
    
    # modifica dello stato della simulazione sul db da "In lavorazione" a "Lavorata"/"Non completata"
    conn = get_connection()
    cur = conn.cursor()    
    cur.execute(f'''
        UPDATE public."SIMULAZIONE" SET "STATO"='{stato_simulazione}' WHERE "ID"={id_simulazione};
    ''')
    conn.commit()
    cur.close()
    conn.close()

    # rimozione cartella temporanea su s3 contenente la lista dei file caricati con IMPORT_DATA
    remove_tmp_folder_on_s3()

    print('statusCode: 200')
