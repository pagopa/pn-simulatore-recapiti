"""
Utility comuni per i job Glue del simulatore recapiti
"""
import boto3
import json
from typing import Dict, Any


def get_db_credentials(secret_id: str) -> Dict[str, str]:
    """
    Recupera le credenziali del database da AWS Secrets Manager
    
    Args:
        secret_id: ID del secret in Secrets Manager
        
    Returns:
        Dict con username e password
    """
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_id)
    secret_string = json.loads(response['SecretString'])
    
    return {
        'username': secret_string.get('username'),
        'password': secret_string.get('password')
    }


def get_jdbc_properties(secret_id: str) -> Dict[str, str]:
    """
    Crea le properties JDBC con le credenziali da Secrets Manager
    
    Args:
        secret_id: ID del secret in Secrets Manager
        
    Returns:
        Dict con user e password per JDBC
    """
    creds = get_db_credentials(secret_id)
    
    return {
        'user': creds['username'],
        'password': creds['password']
    }
