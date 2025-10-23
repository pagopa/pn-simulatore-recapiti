from django.shortcuts import render, redirect

import plotly.graph_objects as go
import json
import plotly.utils as putils
import plotly.express as px
import pandas as pd
import numpy as np
import os
from PagoPA.settings import *

from datetime import datetime

from .models import *

from django.db.models.functions import TruncMonth

import locale

from django.http import JsonResponse

import psycopg2

locale.setlocale(locale.LC_ALL, 'it_IT')


def homepage(request):
    ####### PAGINA PROVVISORIA DI AGGIUNTA DATI #######
    aggiungi_dati()
    ####### PAGINA PROVVISORIA DI AGGIUNTA DATI #######
    lista_simulazioni = table_simulazione.objects.exclude(STATO='Bozza')

    context = {
        'lista_simulazioni': lista_simulazioni
    }
    return render(request, "home.html", context)


def risultati(request, id_simulazione):
    return render(request, "Simulatore/dashboard_risultati.html")

def confronto_risultati(request, id_simulazione):
    return render(request, "Simulatore/dashboard_confronto_risultati.html")


def calendario(request):
    return render(request, "calendario/calendario.html")

def bozze(request):
    lista_bozze = table_simulazione.objects.filter(STATO='Bozza')

    context = {
        'lista_bozze': lista_bozze
    }
    return render(request, "simulazioni/bozze.html", context)

def nuova_simulazione(request, id_simulazione):
    # NUOVA SIMULAZIONE
    if id_simulazione == 'new':
        # Mese da simulare
        lista_mesi_univoci = view_output_capacity_setting.objects.annotate(mese=TruncMonth('ACTIVATION_DATE_FROM')).values_list('mese', flat=True).distinct().order_by('mese')
        lista_mesi_univoci = [(d.strftime("%Y-%m"),d.strftime("%B %Y").capitalize()) for d in lista_mesi_univoci]
        context = {
             'lista_mesi_univoci': lista_mesi_univoci
        }
    # MODIFICA SIMULAZIONE
    else:
        simulazione = table_simulazione.objects.get(ID = id_simulazione)
        context = {
            'simulazione': simulazione
        }
    return render(request, "simulazioni/nuova_simulazione.html", context)

def salva_simulazione(request):
    nome_simulazione = request.POST['nome_simulazione']
    descrizione_simulazione = request.POST['descrizione_simulazione']
    if 'inlineRadioOptions' in request.POST:
        if request.POST['inlineRadioOptions'] == 'now':
            timestamp_esecuzione = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            tipo_trigger = 'Now'
        elif request.POST['inlineRadioOptions'] == 'schedule':
            timestamp_esecuzione = request.POST['schedule_datetime']
            timestamp_esecuzione = datetime.strptime(timestamp_esecuzione, "%d/%m/%Y %H:%M")
            tipo_trigger = 'Schedule'
    else:
        timestamp_esecuzione = None
        tipo_trigger = None
    
    if request.POST['stato'] == 'Bozza':
        stato = 'Bozza'
    elif request.POST['stato'] == 'Schedulata-Inlavorazione':
        if tipo_trigger == 'Now':
            stato = 'In lavorazione'
        else:
            stato = 'Schedulata'
    
    mese_da_simulare = request.POST['mese_da_simulare']
    tipo_capacita_da_modificare = request.POST['tipo_capacita_da_modificare']

    # NUOVA SIMULAZIONE
    if request.POST['id_simulazione'] == '' or 'id_simulazione' not in request.POST['id_simulazione']: # il primo caso si verifica con il salva_bozza mentre il secondo con avvia scheduling
        id_simulazione_salvata = table_simulazione.objects.create(
            NOME = nome_simulazione,
            DESCRIZIONE = descrizione_simulazione,
            STATO = stato,
            TRIGGER = tipo_trigger,
            TIMESTAMP_ESECUZIONE = timestamp_esecuzione,
            MESE_SIMULAZIONE = mese_da_simulare,
            TIPO_CAPACITA = tipo_capacita_da_modificare
        )
        capacita_json = request.POST.get('capacita_json')
        try:
            capacita_json = json.loads(capacita_json)
        except (TypeError, json.JSONDecodeError):
            capacita_json = {}

        for recapitista, righe_tabella in capacita_json.items():
            for singola_riga in righe_tabella:
                table_capacita_modificate.objects.create(
                    UNIFIED_DELIVERY_DRIVER = recapitista,
                    ACTIVATION_DATE_FROM = datetime.strptime(singola_riga['inizioPeriodoValidita'], '%d/%m/%Y'),
                    ACTIVATION_DATE_TO = datetime.strptime(singola_riga['finePeriodoValidita'], '%d/%m/%Y'),
                    CAPACITY = singola_riga['capacita'],
                    SUM_WEEKLY_ESTIMATE = singola_riga['postalizzazioni_settimanali'].split(' ')[0], # formato: SUM_WEEKLY_ESTIMATE (mensili: SUM_MONTHLY_ESTIMATE)
                    SUM_MONTHLY_ESTIMATE = singola_riga['postalizzazioni_settimanali'].split(' ')[-1][:-1], # formato: SUM_WEEKLY_ESTIMATE (mensili: SUM_MONTHLY_ESTIMATE)
                    REGIONE = singola_riga['regione'],
                    PROVINCE = singola_riga['provincia'],
                    PRODUCT_890 = True if '890' in singola_riga['product'] else False,
                    PRODUCT_AR = True if 'AR' in singola_riga['product'] else False,
                    SIMULAZIONE_ID = id_simulazione_salvata
                )

    # MODIFICA SIMULAZIONE
    else:
        simulazione_da_modificare = table_simulazione.objects.get(ID = request.POST['id_simulazione'])
        simulazione_da_modificare.NOME = nome_simulazione
        simulazione_da_modificare.DESCRIZIONE = descrizione_simulazione
        simulazione_da_modificare.STATO = stato
        simulazione_da_modificare.TRIGGER = tipo_trigger
        simulazione_da_modificare.TIMESTAMP_ESECUZIONE = timestamp_esecuzione
        simulazione_da_modificare.MESE_SIMULAZIONE = mese_da_simulare
        simulazione_da_modificare.TIPO_CAPACITA = tipo_capacita_da_modificare
        simulazione_da_modificare.save()

    if request.POST['stato'] == 'Bozza':
        return redirect("bozze")
    elif request.POST['stato'] == 'Schedulata-Inlavorazione':
        return redirect("home")

def rimuovi_simulazione(request, id_simulazione):
    # il try-catch serve per 2 motivi: 1)evitare che .get non trovi nulla dando errore 2)evitare che .delete() non trovi nulla dando errore
    try:
        simulazione_da_rimuovere = table_simulazione.objects.get(ID=id_simulazione)
        try:
            lista_capacita_modificata_da_rimuovere = table_capacita_modificate.objects.filter(SIMULAZIONE_ID=simulazione_da_rimuovere.ID)
            for singola_capacita in lista_capacita_modificata_da_rimuovere:
                singola_capacita.delete()
        except:
            pass
        simulazione_da_rimuovere.delete()
    except:
        pass

    next_url = request.GET.get('next', '/')  # fallback alla home
    return redirect(next_url)


def login_page(request):
    return render(request, "login_page.html")


# AJAX
def ajax_get_capacita_from_mese(request):
    mese_da_simulare = request.GET['mese_da_simulare_selezionato']
    tipo_capacita_selezionata = request.GET['tipo_capacita_selezionata']
    if request.accepts:
        # mettiamo list() altrimenti ci dà l'errore Object of type QuerySet is not JSON serializable
        lista_capacita_grezze = list(view_output_capacity_setting.objects.filter(ACTIVATION_DATE_FROM__year=mese_da_simulare.split('-')[0], ACTIVATION_DATE_FROM__month=mese_da_simulare.split('-')[1]).values())
        lista_capacita_finali = {}
        for item in lista_capacita_grezze:
            recapitista = item['UNIFIED_DELIVERY_DRIVER']
            regione = item['REGIONE']
            provincia = item['PROVINCE']
            post_weekly_estimate = item['SUM_WEEKLY_ESTIMATE']
            post_monthly_estimate = item['SUM_MONTHLY_ESTIMATE']
            if tipo_capacita_selezionata=='Solo BAU':
                capacity = item['CAPACITY']
            elif tipo_capacita_selezionata=='Solo picco':
                capacity = item['PEAK_CAPACITY']
            activation_date_from = item['ACTIVATION_DATE_FROM']
            activation_date_to = item['ACTIVATION_DATE_TO']
            # PRODUCT TYPE: boolean, valori True/False
            product = ''
            if item['PRODUCT_890']:
                product += '890-'
            if item['PRODUCT_AR']:
                product += 'AR-'
            if product!='':
                product = product[:-1]
            
            if recapitista not in lista_capacita_finali:
                lista_capacita_finali[recapitista] = []
            
            lista_capacita_finali[recapitista].append({
                'regione': regione,
                'provincia': provincia,
                'post_weekly_estimate': post_weekly_estimate,
                'post_monthly_estimate': post_monthly_estimate,
                'product': product,
                'activation_date_from': activation_date_from,
                'activation_date_to': activation_date_to,
                'capacity': capacity
            })        
    return JsonResponse({'context': lista_capacita_finali})



def aggiungi_dati():
    df_declared_capacity = pd.read_csv('./static/data/db_declared_capacity.csv', dtype=str, keep_default_na=False)
    df_sender_limit = pd.read_csv('./static/data/db_sender_limit.csv', dtype=str, keep_default_na=False)
    df_cap_prov_reg = pd.read_csv('./static/data/CAP_PROV_REG.csv', dtype=str, keep_default_na=False)

    conn = psycopg2.connect(database = DATABASES['default']['NAME'],
                            user = DATABASES['default']['USER'],
                            password = DATABASES['default']['PASSWORD'],
                            host = DATABASES['default']['HOST'],
                            port = DATABASES['default']['PORT'])

    cur = conn.cursor()



    cur.execute('select count(*) from public."DECLARED_CAPACITY"')
    count_declared_capacity = cur.fetchone()
    if count_declared_capacity[0] == 0:
        for i in range(0 ,len(df_declared_capacity)):
            values_capacity = (df_declared_capacity['capacity'][i], df_declared_capacity['geoKey'][i], df_declared_capacity['tenderIdGeoKey'][i], df_declared_capacity['product_890'][i], df_declared_capacity['product_AR'][i], df_declared_capacity['product_RS'][i], df_declared_capacity['tenderId'][i], df_declared_capacity['unifiedDeliveryDriver'][i], df_declared_capacity['createdAt'][i], df_declared_capacity['peakCapacity'][i], df_declared_capacity['activationDateFrom'][i], df_declared_capacity['activationDateTo'][i], df_declared_capacity['pk'][i])
            cur.execute('INSERT INTO public."DECLARED_CAPACITY" ("CAPACITY","GEOKEY","TENDER_ID_GEOKEY","PRODUCT_890","PRODUCT_AR","PRODUCT_RS","TENDER_ID","UNIFIED_DELIVERY_DRIVER","CREATED_AT","PEAK_CAPACITY","ACTIVATION_DATE_FROM","ACTIVATION_DATE_TO","PK") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        values_capacity)
    
    cur.execute('select count(*) from public."SENDER_LIMIT"')
    count_sender_limit = cur.fetchone()
    if count_sender_limit[0] == 0:
        for i in range(0 ,len(df_sender_limit)):
            values_senderlimit = (df_sender_limit['pk'][i], df_sender_limit['deliveryDate'][i], df_sender_limit['weeklyEstimate'][i], df_sender_limit['monthlyEstimate'][i], df_sender_limit['originalEstimate'][i], df_sender_limit['paId'][i], df_sender_limit['productType'][i], df_sender_limit['province'][i])
            cur.execute('INSERT INTO public."SENDER_LIMIT" ("PK","DELIVERY_DATE","WEEKLY_ESTIMATE","MONTHLY_ESTIMATE","ORIGINAL_ESTIMATE","PA_ID","PRODUCT_TYPE","PROVINCE") VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
                        values_senderlimit)

    cur.execute('select count(*) from public."CAP_PROV_REG"')
    count_cap_prov_reg = cur.fetchone()
    if count_cap_prov_reg[0] == 0:
        for i in range(0 ,len(df_cap_prov_reg)):
            values_capprovreg = (df_cap_prov_reg['CAP'][i], df_cap_prov_reg['Regione'][i], df_cap_prov_reg['Provincia'][i], df_cap_prov_reg['CodSiglaProvincia'][i], df_cap_prov_reg['Pop_cap'][i], df_cap_prov_reg['Prop_pop_cap'][i])
            cur.execute('INSERT INTO public."CAP_PROV_REG" ("CAP","REGIONE","PROVINCIA","COD_SIGLA_PROVINCIA","POP_CAP","PERCENTUALE_POP_CAP") VALUES (%s, %s, %s, %s, %s, %s)',
                        values_capprovreg)

    conn.commit()
    conn.close()



# ERROR PAGES
def handle_error_400(request, exception):
    return render(request, 'error_pages/error_400.html')
def handle_error_403(request, exception):
    return render(request, 'error_pages/error_403.html')
def handle_error_404(request, exception):
    return render(request, 'error_pages/error_404.html')
def handle_error_500(request, *args, **argv):
    return render(request, "error_pages/error_500.html", status=500)