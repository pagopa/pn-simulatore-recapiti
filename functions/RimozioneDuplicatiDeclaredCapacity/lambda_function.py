"""
AWS Lambda che viene invocata alla bisogna e si occupa di rimuovere i duplicati nella tabella del db denominata "DECLARED_CAPACITY"

Trigger:
    Manuale
"""
import json
import boto3
import pg8000
import os

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
    # recupero credenziali da SecretsManager
    creds = recupero_credenziali_db(secretsManager_SecretId)
    # connessione db
    conn = connessione_db(db_host, db_name, db_port, creds)
    # query
    cur = conn.cursor()
    cur.execute(
        '''
            WITH CTE AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                    PARTITION BY public."DECLARED_CAPACITY"."GEOKEY",  public."DECLARED_CAPACITY"."UNIFIED_DELIVERY_DRIVER",  public."DECLARED_CAPACITY"."PRODUCT_890",
                public."DECLARED_CAPACITY"."PRODUCT_AR" ,  public."DECLARED_CAPACITY"."PRODUCT_RS",  public."DECLARED_CAPACITY"."ACTIVATION_DATE_FROM"
                                    ORDER BY  public."DECLARED_CAPACITY"."LAST_UPDATE_TIMESTAMP" DESC
                                    ) AS "rn"
            FROM public."DECLARED_CAPACITY"
            )
            DELETE FROM public."DECLARED_CAPACITY"
            WHERE public."DECLARED_CAPACITY"."PK" IN (SELECT "PK" FROM CTE WHERE "rn" > 1);
        '''
    )
    conn.commit()
    cur.close()
    conn.close()

    return {'statusCode': 200}
 