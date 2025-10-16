#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys

import psycopg2
import pandas as pd


def main():
    """Run administrative tasks."""
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'PagoPA.settings')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    
    df_declared_capacity = pd.read_csv('./static/data/db_declared_capacity.csv', dtype=str)
    df_sender_limit = pd.read_csv('./static/data/db_sender_limit.csv', dtype=str, keep_default_na=False)
    df_cap_prov_reg = pd.read_csv('./static/data/CAP_PROV_REG.csv', dtype=str, keep_default_na=False)

    conn = psycopg2.connect(database = "db_simulatore",
                            user = "postgres",
                            password = "a",
                            host = "127.0.0.1",
                            port = "5432")

    cur = conn.cursor()



    cur.execute('select count(*) from public."DECLARED_CAPACITY"')
    count_declared_capacity = cur.fetchone()
    if count_declared_capacity[0] == 0:
        for i in range(0 ,len(df_declared_capacity)):
            values_capacity = (df_declared_capacity['unifiedDeliveryDriverGeokey'][i], df_declared_capacity['deliveryDate'][i], df_declared_capacity['geoKey'][i], df_declared_capacity['unifiedDeliveryDriver'][i], df_declared_capacity['usedCapacity'][i], df_declared_capacity['capacity'][i])
            cur.execute('INSERT INTO public."DECLARED_CAPACITY" ("UNIFIEDDELIVERYDRIVERGEOKEY","DELIVERYDATE","GEOKEY","UNIFIEDDELIVERYDRIVER","USEDCAPACITY","CAPACITY") VALUES (%s, %s, %s, %s, %s, %s)',
                        values_capacity)
    
    cur.execute('select count(*) from public."SENDER_LIMIT"')
    count_sender_limit = cur.fetchone()
    if count_sender_limit[0] == 0:
        for i in range(0 ,len(df_sender_limit)):
            values_senderlimit = (df_sender_limit['pk'][i], df_sender_limit['deliveryDate'][i], df_sender_limit['weeklyEstimate'][i], df_sender_limit['monthlyEstimate'][i], df_sender_limit['originalEstimate'][i], df_sender_limit['paId'][i], df_sender_limit['productType'][i], df_sender_limit['province'][i])
            cur.execute('INSERT INTO public."SENDER_LIMIT" ("PK","DELIVERYDATE","WEEKLYESTIMATE","MONTHLYESTIMATE","ORIGINALESTIMATE","PAID","PRODUCTTYPE","PROVINCE") VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
                        values_senderlimit)

    cur.execute('select count(*) from public."CAP_PROV_REG"')
    count_cap_prov_reg = cur.fetchone()
    if count_cap_prov_reg[0] == 0:
        for i in range(0 ,len(df_cap_prov_reg)):
            values_capprovreg = (df_cap_prov_reg['CAP'][i], df_cap_prov_reg['Regione'][i], df_cap_prov_reg['Provincia'][i], df_cap_prov_reg['CodSiglaProvincia'][i], df_cap_prov_reg['DescrMacroregione'][i])
            cur.execute('INSERT INTO public."CAP_PROV_REG" ("CAP","REGIONE","PROVINCIA","CODSIGLAPROVINCIA","DESCRMACROREGIONE") VALUES (%s, %s, %s, %s, %s)',
                        values_capprovreg)

    conn.commit()
    conn.close()

    main()
