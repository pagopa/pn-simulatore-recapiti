"""
AWS Lambda che viene invocata alla bisogna e si occupa di fare pulizia nelle tabelle del db denominate "OUTPUT_GRAFICO_ENTE" e "OUTPUT_GRAFICO_REG_RECAP"

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
                    PARTITION BY public."OUTPUT_GRAFICO_ENTE"."SIMULAZIONE_ID",  public."OUTPUT_GRAFICO_ENTE"."SENDER_PA_ID",  public."OUTPUT_GRAFICO_ENTE"."SETTIMANA_DELIVERY" 
                                    ORDER BY  public."OUTPUT_GRAFICO_ENTE"."SIMULAZIONE_ID" DESC
                                    ) AS "rn"
                FROM public."OUTPUT_GRAFICO_ENTE"
            )
            DELETE FROM public."OUTPUT_GRAFICO_ENTE"
            WHERE public."OUTPUT_GRAFICO_ENTE"."ID" IN (SELECT "ID" FROM CTE WHERE "rn" > 1);
        '''
    )
    conn.commit()
    cur.execute(
        '''
            WITH CTE AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                    PARTITION BY public."OUTPUT_GRAFICO_REG_RECAP"."SIMULAZIONE_ID",  public."OUTPUT_GRAFICO_REG_RECAP"."PROVINCE",public."OUTPUT_GRAFICO_REG_RECAP"."REGIONE", public."OUTPUT_GRAFICO_REG_RECAP"."UNIFIED_DELIVERY_DRIVER",public."OUTPUT_GRAFICO_REG_RECAP"."SETTIMANA_DELIVERY" 
                                    ORDER BY  public."OUTPUT_GRAFICO_REG_RECAP"."SIMULAZIONE_ID" DESC
                                    ) AS "rn"
                FROM public."OUTPUT_GRAFICO_REG_RECAP"
            )
            DELETE FROM public."OUTPUT_GRAFICO_REG_RECAP"
            WHERE public."OUTPUT_GRAFICO_REG_RECAP"."ID" IN (SELECT "ID" FROM CTE WHERE "rn" > 1);
        '''
    )
    conn.commit()
    cur.close()
    conn.close()

    return {'statusCode': 200}
 