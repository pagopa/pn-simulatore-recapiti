import json
import boto3
import pg8000
import os

def get_db_credentials(secretsManager_SecretId):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secretsManager_SecretId)
    response_SecretString = json.loads(response['SecretString'])
    return response_SecretString


def get_connection():
    # recupero credenziali da SecretsManager
    secretsManager_SecretId = os.environ['secretsManager_SecretId']
    creds = get_db_credentials(secretsManager_SecretId)
    conn = pg8000.connect(
        host=os.environ['DB_HOST'],
        database=os.environ['DB_NAME'],
        user=creds["username"],
        password=creds["password"],
        port=os.environ['DB_PORT']
    )
    return conn


def lambda_handler(event, context):
    
    # recupero mese_simulazione dai parametri di input
    mese_simulazione = event['mese_simulazione']
    # connessione db
    conn = get_connection()
    # query
    cur = conn.cursor()    
    cur.execute('SELECT COUNT(*) FROM public."SENDER_LIMIT_DELTA"')
    count_rows_delta_table = int(cur.fetchone()[0])
    if count_rows_delta_table == 0:
        raise Exception(f"Errore: tabella SENDER_LIMIT vuota per il mese di {mese_simulazione[:-3]}")
    cur.execute(
        '''
            MERGE INTO public."SENDER_LIMIT"
            USING public."SENDER_LIMIT_DELTA"
            ON (public."SENDER_LIMIT"."PK" = public."SENDER_LIMIT_DELTA"."PK" 
                AND public."SENDER_LIMIT"."DELIVERY_DATE" = public."SENDER_LIMIT_DELTA"."DELIVERY_DATE") 
            --When records are matched, update the records if there is any change
            WHEN MATCHED AND public."SENDER_LIMIT"."LAST_UPDATE_TIMESTAMP" < public."SENDER_LIMIT_DELTA"."LAST_UPDATE_TIMESTAMP" 
            THEN UPDATE SET 
            "PK" = public."SENDER_LIMIT_DELTA"."PK", "DELIVERY_DATE" = public."SENDER_LIMIT_DELTA"."DELIVERY_DATE",
            "WEEKLY_ESTIMATE" = public."SENDER_LIMIT_DELTA"."WEEKLY_ESTIMATE", "MONTHLY_ESTIMATE" = public."SENDER_LIMIT_DELTA"."MONTHLY_ESTIMATE", 
            "ORIGINAL_ESTIMATE" = public."SENDER_LIMIT_DELTA"."ORIGINAL_ESTIMATE", "PA_ID" = public."SENDER_LIMIT_DELTA"."PA_ID", 
            "PRODUCT_TYPE" = public."SENDER_LIMIT_DELTA"."PRODUCT_TYPE", "PROVINCE" = public."SENDER_LIMIT_DELTA"."PROVINCE", 
            "LAST_UPDATE_TIMESTAMP" = public."SENDER_LIMIT_DELTA"."LAST_UPDATE_TIMESTAMP"
            WHEN NOT MATCHED 
            THEN INSERT ("PK", "DELIVERY_DATE", "WEEKLY_ESTIMATE", "MONTHLY_ESTIMATE", 
            "ORIGINAL_ESTIMATE", "PA_ID", "PRODUCT_TYPE", "PROVINCE", "LAST_UPDATE_TIMESTAMP") 
            VALUES 
            (public."SENDER_LIMIT_DELTA"."PK",public."SENDER_LIMIT_DELTA"."DELIVERY_DATE",
            public."SENDER_LIMIT_DELTA"."WEEKLY_ESTIMATE", public."SENDER_LIMIT_DELTA"."MONTHLY_ESTIMATE", 
            public."SENDER_LIMIT_DELTA"."ORIGINAL_ESTIMATE", public."SENDER_LIMIT_DELTA"."PA_ID", 
            public."SENDER_LIMIT_DELTA"."PRODUCT_TYPE", public."SENDER_LIMIT_DELTA"."PROVINCE", 
            public."SENDER_LIMIT_DELTA"."LAST_UPDATE_TIMESTAMP");
        '''
    )
    conn.commit()
    cur.execute('DELETE FROM public."SENDER_LIMIT_DELTA";')
    conn.commit()
    cur.close()
    conn.close()

    return {'statusCode': 200}
