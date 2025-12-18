import json
import boto3
import pg8000
import csv
import io
import os

def read_csv_from_s3(bucket, key):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read().decode("utf-8")
    return list(csv.reader(io.StringIO(content)))

def get_db_credentials(secretsManager_SecretId):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secretsManager_SecretId)
    response_SecretString = json.loads(response['SecretString'])
    return response_SecretString


def get_connection(db_host, db_name, db_port, creds):
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
    # recupero credenziali da SecretsManager
    creds = get_db_credentials(secretsManager_SecretId)
    # connessione db
    conn = get_connection(db_host, db_name, db_port, creds)

    # recupero record da csv salvato su s3
    rows = read_csv_from_s3("pn-simulatore-recapiti-hesplora-dev", "dataset_db/regione_provincia_cap.csv")
    # rimozione header
    rows = rows[1:]

    #query
    cur = conn.cursor()
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
