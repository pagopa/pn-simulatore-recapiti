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
    mese_simulazione = event['mese_simulazione']
    # recupero credenziali da SecretsManager
    creds = get_db_credentials(secretsManager_SecretId)
    # connessione db
    conn = get_connection(db_host, db_name, db_port, creds)
    # query
    cur = conn.cursor()   
    cur.execute('SELECT COUNT(*) FROM public."DECLARED_CAPACITY_DELTA"')
    count_rows_delta_table = int(cur.fetchone()[0])
    if count_rows_delta_table == 0:
        raise Exception(f"Errore: tabella DECLARED_CAPACITY vuota per il mese di {mese_simulazione[:-3]}") 
    cur.execute(
        '''
            MERGE INTO public."DECLARED_CAPACITY"
            USING public."DECLARED_CAPACITY_DELTA"
            ON (public."DECLARED_CAPACITY"."GEOKEY" = public."DECLARED_CAPACITY_DELTA"."GEOKEY" AND
                public."DECLARED_CAPACITY"."UNIFIED_DELIVERY_DRIVER" = public."DECLARED_CAPACITY_DELTA"."UNIFIED_DELIVERY_DRIVER" AND
                public."DECLARED_CAPACITY"."PRODUCT_890" = public."DECLARED_CAPACITY_DELTA"."PRODUCT_890" AND
                public."DECLARED_CAPACITY"."PRODUCT_AR" = public."DECLARED_CAPACITY_DELTA"."PRODUCT_AR" AND
                public."DECLARED_CAPACITY"."PRODUCT_RS" = public."DECLARED_CAPACITY_DELTA"."PRODUCT_RS" AND
                public."DECLARED_CAPACITY"."ACTIVATION_DATE_FROM" = public."DECLARED_CAPACITY_DELTA"."ACTIVATION_DATE_FROM") 
            --When records are matched, update the records if there is any change
            WHEN MATCHED AND public."DECLARED_CAPACITY"."LAST_UPDATE_TIMESTAMP" < public."DECLARED_CAPACITY_DELTA"."LAST_UPDATE_TIMESTAMP" 
            THEN UPDATE SET 
            "PK" = public."DECLARED_CAPACITY_DELTA"."PK", 
            "CAPACITY" = public."DECLARED_CAPACITY_DELTA"."CAPACITY", "GEOKEY" = public."DECLARED_CAPACITY_DELTA"."GEOKEY", 
            "TENDER_ID_GEOKEY" = public."DECLARED_CAPACITY_DELTA"."TENDER_ID_GEOKEY", "PRODUCT_890" = public."DECLARED_CAPACITY_DELTA"."PRODUCT_890", 
            "PRODUCT_AR" = public."DECLARED_CAPACITY_DELTA"."PRODUCT_AR", "PRODUCT_RS" = public."DECLARED_CAPACITY_DELTA"."PRODUCT_RS", 
            "TENDER_ID" = public."DECLARED_CAPACITY_DELTA"."TENDER_ID", "UNIFIED_DELIVERY_DRIVER" = public."DECLARED_CAPACITY_DELTA"."UNIFIED_DELIVERY_DRIVER", 
            "CREATED_AT" = public."DECLARED_CAPACITY_DELTA"."CREATED_AT", "PEAK_CAPACITY" = public."DECLARED_CAPACITY_DELTA"."PEAK_CAPACITY", 
            "ACTIVATION_DATE_FROM" = public."DECLARED_CAPACITY_DELTA"."ACTIVATION_DATE_FROM", "ACTIVATION_DATE_TO" = public."DECLARED_CAPACITY_DELTA"."ACTIVATION_DATE_TO", 
            "PRODUCTION_CAPACITY" = public."DECLARED_CAPACITY_DELTA"."PRODUCTION_CAPACITY", "LAST_UPDATE_TIMESTAMP" = public."DECLARED_CAPACITY_DELTA"."LAST_UPDATE_TIMESTAMP"
            WHEN NOT MATCHED 
            THEN INSERT ("PK", "CAPACITY" , "GEOKEY" , "TENDER_ID_GEOKEY" , "PRODUCT_890" , "PRODUCT_AR" , "PRODUCT_RS" ,
            "TENDER_ID" , "UNIFIED_DELIVERY_DRIVER" , "CREATED_AT" , "PEAK_CAPACITY" , "ACTIVATION_DATE_FROM" ,
            "ACTIVATION_DATE_TO" , "PRODUCTION_CAPACITY" , "LAST_UPDATE_TIMESTAMP" ) 
            VALUES 
            (public."DECLARED_CAPACITY_DELTA"."PK", public."DECLARED_CAPACITY_DELTA"."CAPACITY", public."DECLARED_CAPACITY_DELTA"."GEOKEY", 
            public."DECLARED_CAPACITY_DELTA"."TENDER_ID_GEOKEY", public."DECLARED_CAPACITY_DELTA"."PRODUCT_890", 
            public."DECLARED_CAPACITY_DELTA"."PRODUCT_AR", public."DECLARED_CAPACITY_DELTA"."PRODUCT_RS", 
            public."DECLARED_CAPACITY_DELTA"."TENDER_ID", public."DECLARED_CAPACITY_DELTA"."UNIFIED_DELIVERY_DRIVER", 
            public."DECLARED_CAPACITY_DELTA"."CREATED_AT", public."DECLARED_CAPACITY_DELTA"."PEAK_CAPACITY", 
            public."DECLARED_CAPACITY_DELTA"."ACTIVATION_DATE_FROM", public."DECLARED_CAPACITY_DELTA"."ACTIVATION_DATE_TO", 
            public."DECLARED_CAPACITY_DELTA"."PRODUCTION_CAPACITY", public."DECLARED_CAPACITY_DELTA"."LAST_UPDATE_TIMESTAMP");
        '''
    )
    conn.commit()
    cur.execute('DELETE FROM public."DECLARED_CAPACITY_DELTA";')
    conn.commit()
    cur.close()
    conn.close()

    return {'statusCode': 200}
 