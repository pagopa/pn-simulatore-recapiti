"""
AWS Lambda che si occupa di effettuare l'operazione di MERGE fra le tabelle del db denominate "SENDER_LIMIT_DELTA" e "SENDER_LIMIT"

Trigger:
    Step function pn-simulatore-recapiti-sf-RecuperoDati-Weekly

Input:
    mese_simulazione: prima settimana del mese di simulazione, nel formato YYYY-MM-DD
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
    # recupero mese_simulazione dai parametri di input
    mese_simulazione = event['mese_simulazione']
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
    cur.execute('SELECT COUNT(*) FROM public."SENDER_LIMIT_DELTA"')
    count_rows_delta_table = int(cur.fetchone()[0])
    if count_rows_delta_table == 0:
        raise Exception(f"Errore: tabella SENDER_LIMIT vuota per il mese di {mese_simulazione[:-3]}")
    cur.execute(
        '''
            MERGE INTO public."SENDER_LIMIT"
            USING public."SENDER_LIMIT_DELTA"
            ON (public."SENDER_LIMIT"."PA_ID" = public."SENDER_LIMIT_DELTA"."PA_ID"
                AND public."SENDER_LIMIT"."PROVINCE" = public."SENDER_LIMIT_DELTA"."PROVINCE"
                AND public."SENDER_LIMIT"."PRODUCT_TYPE" = public."SENDER_LIMIT_DELTA"."PRODUCT_TYPE"
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
    cur.close()
    conn.close()

    return {'statusCode': 200}
 