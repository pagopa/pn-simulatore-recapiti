"""
AWS Lambda che viene invocata alla bisogna e si occupa di fare upsert nella tabella del db denominata "CAP_PROV_REG"

Trigger:
    Manuale, alla prima installazione o in corrispondenza di un aggiornamento su CAP/province/regioni
"""
import json
import boto3
import pg8000
import csv
import io
import os

def lettura_csv_s3(bucket, key):
    """
    Recupero i record da un file csv salvato su un bucket s3

    Args:
        bucket (string): nome del bucket s3
        key (string): nome della key all'interno del bucket s3 che mi permette di raggiungere il file

    Returns:
        list: record recuperati dal file csv salvato sul bucket s3
    """
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read().decode("utf-8")
    return list(csv.reader(io.StringIO(content)))

def recupero_credenziali_db(secretsManager_SecretId):
    """
    Recupera le credenziali di connessione al db salvate sul secret manager

    Args:
        secretsManager_SecretId (string): arn dell'istanza secret manager che contiene le credenziali del db

    Returns:
        dict: credenziali del db recuperate dal secret manager
    """
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secretsManager_SecretId)
    response_SecretString = json.loads(response['SecretString'])
    return response_SecretString


def connessione_db(db_host, db_name, db_port, creds):
    """
    Crea la connessione al db

    Args:
        db_host (string): server del db
        db_name (string): nome del db
        db_port (string): porta del db
        creds (string): contiene le credenziali del db recuperate dal secret manager

    Returns:
        pg8000.legacy.Connection: istanza di connessione al db
    """
    conn = pg8000.connect(
        host=db_host,
        database=db_name,
        user=creds["username"],
        password=creds["password"],
        port=db_port
    )
    return conn

def lambda_handler(event, context):
    # recupero variabili d'ambiente
    secretsManager_SecretId = os.environ['secretsManager_SecretId']
    db_host = os.environ['DB_HOST']
    db_name = os.environ['DB_NAME']
    db_port = os.environ['DB_PORT']
    s3_bucket = os.environ['source_bucket']
    # recupero credenziali da SecretsManager
    creds = recupero_credenziali_db(secretsManager_SecretId)
    # connessione db
    conn = connessione_db(db_host, db_name, db_port, creds)
    # recupero record da csv salvato su s3
    rows = lettura_csv_s3(s3_bucket, "dataset_db/regione_provincia_cap.csv")
    # rimozione header
    rows = rows[1:]
    # cancellazione
    cur = conn.cursor()
    cur.execute('DELETE FROM public."CAP_PROV_REG";')
    # commit della cancellazione
    conn.commit()
    # inserimento
    insert_query = """
        INSERT INTO public."CAP_PROV_REG" ("CAP", "REGIONE", "PROVINCIA", "COD_SIGLA_PROVINCIA", "POP_CAP", "PERCENTUALE_POP_CAP")
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cur.executemany(insert_query, rows)
    # commit dell'inserimento
    conn.commit()
    # chiusura connessione
    cur.close()
    conn.close()
    return {'statusCode': 200}