import json
import boto3
import pg8000
import os

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
 