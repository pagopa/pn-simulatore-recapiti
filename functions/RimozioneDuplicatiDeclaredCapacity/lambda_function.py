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
 