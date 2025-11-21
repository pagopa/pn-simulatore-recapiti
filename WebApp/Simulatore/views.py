from django.shortcuts import render, redirect
import plotly.graph_objects as go
import json
import plotly.utils as putils
import plotly.express as px
import pandas as pd
import numpy as np
import os
from PagoPA.settings import *
from datetime import datetime, timedelta
from datetime import date
from dateutil.relativedelta import relativedelta
from .models import *
from django.db.models import Q
from django.db.models.functions import TruncMonth
from django.http import JsonResponse
import psycopg2
from django.db import connection
from django.utils import timezone
from zoneinfo import ZoneInfo
import locale
locale.setlocale(locale.LC_ALL, 'it_IT.UTF-8')


def homepage(request):
    lista_simulazioni = table_simulazione.objects.exclude(STATO='Bozza')
    
    for singola_simulazione in lista_simulazioni:
        # cambio stato su 'In lavorazione' per schedulata con timestamp_esecuzione <= now()
        if singola_simulazione.STATO=='Schedulata' and singola_simulazione.TRIGGER=='Schedule' and singola_simulazione.TIMESTAMP_ESECUZIONE <= timezone.now():
            singola_simulazione.STATO = 'In lavorazione'
        # Get ID per confronto con automatizzata
        singola_simulazione.automatizzata_da_confrontare = None
        monday_current_week = singola_simulazione.TIMESTAMP_ESECUZIONE.date() - timedelta(days=singola_simulazione.TIMESTAMP_ESECUZIONE.weekday())
        if singola_simulazione.TIPO_SIMULAZIONE == 'Automatizzata':
            previous_week_monday = monday_current_week - timedelta(days=7)
            if previous_week_monday.month == monday_current_week.month:
                simulazione_recuperata = table_simulazione.objects.filter(TIPO_SIMULAZIONE='Automatizzata',STATO='Lavorata',TIMESTAMP_ESECUZIONE__date=previous_week_monday).first()
                if simulazione_recuperata:
                    singola_simulazione.automatizzata_da_confrontare = simulazione_recuperata.ID
        elif singola_simulazione.TIPO_SIMULAZIONE == 'Manuale':
            if singola_simulazione.TIMESTAMP_ESECUZIONE.month == monday_current_week.month:
                simulazione_recuperata = table_simulazione.objects.filter(TIPO_SIMULAZIONE='Automatizzata',STATO='Lavorata',TIMESTAMP_ESECUZIONE__year=monday_current_week.year,TIMESTAMP_ESECUZIONE__month=monday_current_week.month).order_by("-TIMESTAMP_ESECUZIONE").first()
                if simulazione_recuperata:
                    singola_simulazione.automatizzata_da_confrontare = simulazione_recuperata.ID

    context = {
        'lista_simulazioni': lista_simulazioni
    }
    return render(request, "home.html", context)


def risultati(request, id_simulazione):
    return render(request, "Simulatore/dashboard_risultati.html")

def confronto_risultati(request, id1, id2):
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
    # Mese da simulare
    lista_mesi = get_mesi_distinct()

    lista_regioni = table_cap_prov_reg.objects.values_list('REGIONE', flat=True).distinct().order_by('REGIONE')

    context = {
        'lista_mesi': lista_mesi,
        'lista_regioni': lista_regioni
    }
    # New_from_old
    new_from_old = None
    if 'id' in request.GET:
        new_from_old = request.GET['id']
    # NUOVA SIMULAZIONE
    if id_simulazione == 'new' and new_from_old == None:
        pass
    # New_from_old
    elif id_simulazione == 'new' and new_from_old != None:
        simulazione_selezionata = table_simulazione.objects.get(ID = new_from_old)
        simulazione_selezionata.new_from_old = True
        context['simulazione_selezionata'] = simulazione_selezionata
    # MODIFICA SIMULAZIONE
    else:
        simulazione_selezionata = table_simulazione.objects.get(ID = id_simulazione)
        simulazione_selezionata.new_from_old = False
        context['simulazione_selezionata'] = simulazione_selezionata
    return render(request, "simulazioni/nuova_simulazione.html", context)

def salva_simulazione(request):
    tipo_simulazione = 'Manuale'
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
    
    mese_da_simulare = None
    if 'mese_da_simulare' in request.POST:
        mese_da_simulare = request.POST['mese_da_simulare']
    tipo_capacita_da_modificare = None
    if 'tipo_capacita_da_modificare' in request.POST:
        tipo_capacita_da_modificare = request.POST['tipo_capacita_da_modificare']
    
    # recuperiamo le capacità modificate dall'utente
    capacita_json = request.POST.get('capacita_json')
    try:
        capacita_json = json.loads(capacita_json)
    except (TypeError, json.JSONDecodeError):
        capacita_json = {}

    # NUOVA SIMULAZIONE o new_from_old
    if request.POST['id_simulazione'] == '' or 'id_simulazione' not in request.POST or request.POST['new_from_old']=='True': # la prima condizione si verifica con il salva_bozza, la seconda condizione si verifica con avvia scheduling, la terza con new_from_old
        id_simulazione_salvata = table_simulazione.objects.create(
            NOME = nome_simulazione,
            DESCRIZIONE = descrizione_simulazione,
            STATO = stato,
            TRIGGER = tipo_trigger,
            TIMESTAMP_ESECUZIONE = timestamp_esecuzione,
            MESE_SIMULAZIONE = mese_da_simulare,
            TIPO_CAPACITA = tipo_capacita_da_modificare,
            TIPO_SIMULAZIONE = tipo_simulazione
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
        simulazione_da_modificare.TIPO_SIMULAZIONE = tipo_simulazione
        simulazione_da_modificare.save()
        id_simulazione_salvata = simulazione_da_modificare
        

    # SALVATAGGIO CAPACITÀ MODIFICATE DALL'UTENTE
    if mese_da_simulare != None and tipo_capacita_da_modificare != None:
        lista_old_capacita_modificate = table_capacita_simulate.objects.filter(SIMULAZIONE_ID = id_simulazione_salvata)
        if lista_old_capacita_modificate:
            # modifica delle CAPACITA_SIMULATE salvate su db
            lookup = {}
            for recapitista, righe_tabella in capacita_json.items():
                for row in righe_tabella:
                    lookup[(recapitista, row["cod_sigla_provincia"], row['inizioPeriodoValidita'])] = row["capacita"]

            for singola_capacita in lista_old_capacita_modificate:
                key = (singola_capacita.UNIFIED_DELIVERY_DRIVER, singola_capacita.COD_SIGLA_PROVINCIA, singola_capacita.ACTIVATION_DATE_FROM)
                if key in lookup:
                    singola_capacita.CAPACITY = lookup[key]    
            
            table_capacita_simulate.objects.bulk_update(lista_old_capacita_modificate, ["CAPACITY"])
        else:
            # scrittura sul db nella tabella CAPACITA_SIMULATE
            for recapitista, righe_tabella in capacita_json.items():
                for singola_riga in righe_tabella:
                    table_capacita_simulate.objects.create(
                        UNIFIED_DELIVERY_DRIVER = recapitista,
                        ACTIVATION_DATE_FROM = datetime.strptime(singola_riga['inizioPeriodoValidita']+' 00:00:00', '%d/%m/%Y %H:%M:%S'),
                        ACTIVATION_DATE_TO = datetime.strptime(singola_riga['finePeriodoValidita']+' 23:59:59', '%d/%m/%Y %H:%M:%S'),
                        CAPACITY = singola_riga['capacita'],
                        SUM_WEEKLY_ESTIMATE = singola_riga['postalizzazioni_settimanali'],
                        REGIONE = singola_riga['regione'],
                        COD_SIGLA_PROVINCIA = singola_riga['cod_sigla_provincia'],
                        PRODUCT_890 = True if '890' in singola_riga['product'] else False,
                        PRODUCT_AR = True if 'AR' in singola_riga['product'] else False,
                        LAST_UPDATE_TIMESTAMP = datetime.now(ZoneInfo("Europe/Rome")).strftime('%Y-%m-%d %H:%M:%S'),
                        SIMULAZIONE_ID = id_simulazione_salvata
                    )

    if request.POST['stato'] == 'Bozza':
        return redirect("bozze")
    elif request.POST['stato'] == 'Schedulata-Inlavorazione':
        return redirect("home")

def rimuovi_simulazione(request, id_simulazione):
    # il try-catch serve per 2 motivi: 1)evitare che .get non trovi nulla dando errore 2)evitare che .delete() non trovi nulla dando errore
    try:
        simulazione_da_rimuovere = table_simulazione.objects.get(ID=id_simulazione)
        try:
            lista_capacita_modificata_da_rimuovere = table_capacita_simulate.objects.filter(SIMULAZIONE_ID=simulazione_da_rimuovere.ID)
            for singola_capacita in lista_capacita_modificata_da_rimuovere:
                singola_capacita.delete()
        except:
            pass
        simulazione_da_rimuovere.delete()
    except:
        pass

    next_url = request.GET.get('next', '/')  # fallback alla home
    return redirect(next_url)


def carica_dati_db(request):
    ####### PAGINA PROVVISORIA DI AGGIUNTA DATI #######
    df_declared_capacity = pd.read_csv('static/data/db_declared_capacity.csv', dtype=str, keep_default_na=False)
    df_sender_limit = pd.read_csv('static/data/db_sender_limit.csv', dtype=str, keep_default_na=False)
    df_cap_prov_reg = pd.read_csv('static/data/regione_provincia_cap.csv', dtype=str, keep_default_na=False)


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
    return redirect("status")


def svuota_tabelle_db(request):
    ####### PAGINA PROVVISORIA CHE SVUOTA TUTTE LE TABELLE DEL DB SENZA ELIMINARLE #######
    conn = psycopg2.connect(database = DATABASES['default']['NAME'],
                            user = DATABASES['default']['USER'],
                            password = DATABASES['default']['PASSWORD'],
                            host = DATABASES['default']['HOST'],
                            port = DATABASES['default']['PORT'])

    conn.autocommit = True
    cur = conn.cursor()

    tabelle_da_eliminare = ['CAPACITA_SIMULATE','DECLARED_CAPACITY','SENDER_LIMIT','CAP_PROV_REG','SIMULAZIONE']

    # svuotiamo le tabelle
    for singola_tabella in tabelle_da_eliminare:
        cur.execute(f"TRUNCATE TABLE \"{singola_tabella}\" RESTART IDENTITY CASCADE;")

    cur.close()
    conn.close()

    return redirect("status")


def svuota_db(request):
    ####### PAGINA PROVVISORIA CHE SVUOTA TUTTO IL DB #######
    conn = psycopg2.connect(database = DATABASES['default']['NAME'],
                            user = DATABASES['default']['USER'],
                            password = DATABASES['default']['PASSWORD'],
                            host = DATABASES['default']['HOST'],
                            port = DATABASES['default']['PORT'])
    cur = conn.cursor()
    cur.execute(f'DROP SCHEMA public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO {DATABASES['default']['USER']}; GRANT ALL ON SCHEMA public TO public;')
    conn.commit()
    conn.close()
    return redirect("status")

# AJAX
def ajax_get_capacita_from_mese_and_tipo(request):
    mese_da_simulare = request.GET['mese_da_simulare_selezionato']
    # calcolo del primo lunedì del mese successivo al mese selezionato dall'utente per la simulazione
    anno, mese = map(int, mese_da_simulare.split('-'))
    primo_lunedi_mese_successivo = date(anno, mese, 1) + relativedelta(months=+1)
    offset = (0 - primo_lunedi_mese_successivo.weekday()) % 7 # RICORDA: con weekday(), 0=lunedì, 6=domenica
    primo_lunedi_mese_successivo = str(primo_lunedi_mese_successivo + timedelta(days=offset))

    tipo_capacita_selezionata = request.GET['tipo_capacita_selezionata']
    id_simulazione = request.GET['id_simulazione']
    get_modified_capacity = request.GET['get_modified_capacity']
    if request.accepts:
        if id_simulazione == '' or get_modified_capacity=='false':
            lista_capacita_grezze = list(view_output_capacity_setting.objects.filter(Q(ACTIVATION_DATE_FROM__year=mese_da_simulare.split('-')[0], ACTIVATION_DATE_FROM__month=mese_da_simulare.split('-')[1]) | Q(ACTIVATION_DATE_FROM__year=primo_lunedi_mese_successivo.split('-')[0], ACTIVATION_DATE_FROM__month=primo_lunedi_mese_successivo.split('-')[1], ACTIVATION_DATE_FROM__day=primo_lunedi_mese_successivo.split('-')[2])).order_by('UNIFIED_DELIVERY_DRIVER','REGIONE','PROVINCIA','ACTIVATION_DATE_FROM').values())
            nuova_simulazione = True
        else:
            # RECUPERIAMO LE CAPACITÀ DA UNA SIMULAZIONE ESISTENTE (per modifica simulazione, modifica bozza o nuova simulazione partendo dallo stesso input)
            lista_capacita_grezze = list(view_output_modified_capacity_setting.objects.filter(SIMULAZIONE_ID = id_simulazione).order_by('UNIFIED_DELIVERY_DRIVER','REGIONE','PROVINCIA','ACTIVATION_DATE_FROM').values())
            nuova_simulazione = False
        lista_capacita_finali = {}
        for item in lista_capacita_grezze:
            recapitista = item['UNIFIED_DELIVERY_DRIVER']
            regione = item['REGIONE']
            cod_sigla_provincia = item['COD_SIGLA_PROVINCIA']
            provincia = item['PROVINCIA']
            post_weekly_estimate = item['SUM_WEEKLY_ESTIMATE']
            post_monthly_estimate = item['SUM_MONTHLY_ESTIMATE']
            production_capacity = item['PRODUCTION_CAPACITY']
            if nuova_simulazione:
                if tipo_capacita_selezionata=='BAU':
                    capacity = item['CAPACITY']
                elif tipo_capacita_selezionata=='Picco':
                    capacity = item['PEAK_CAPACITY']
                elif tipo_capacita_selezionata == 'Combinata':
                    # REGOLA: quando i volumi sono inferiori alla BAU setta BAU mentre se i volumi sono superiori alla BAU o al picco setta picco.
                    if post_weekly_estimate < item['CAPACITY']:
                        capacity = item['CAPACITY']
                    else:
                        capacity = item['PEAK_CAPACITY']
                # qui è solo fittizia; è fondamentale quando non abbiamo una nuova simulazione
                original_capacity = capacity
            else:
                original_capacity = item['ORIGINAL_CAPACITY']
                capacity = item['MODIFIED_CAPACITY']
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
                lista_capacita_finali[recapitista] = {}
            if regione+'_'+cod_sigla_provincia+'_'+product not in lista_capacita_finali[recapitista]:
                lista_capacita_finali[recapitista][regione+'_'+cod_sigla_provincia+'_'+product] = []
            
            lista_capacita_finali[recapitista][regione+'_'+cod_sigla_provincia+'_'+product].append(
                {
                    'regione': regione,
                    'provincia': provincia,
                    'cod_sigla_provincia': cod_sigla_provincia,
                    'post_weekly_estimate': post_weekly_estimate,
                    'post_monthly_estimate': post_monthly_estimate,
                    'product': product,
                    'activation_date_from': activation_date_from,
                    'activation_date_to': activation_date_to,
                    'capacity': capacity,
                    'production_capacity': production_capacity,
                    'original_capacity': original_capacity
                }
            )

    '''
    STRUTTURA lista_capacita_finali:
    
    '''
    return JsonResponse({'context': lista_capacita_finali})



def ajax_get_simulazioni_da_confrontare(request):
    id_simulazione = request.GET['id_simulazione']
    mese_simulazione = request.GET['mese_simulazione']
    lista_simulazioni_da_confrontare = list(table_simulazione.objects.filter(STATO='Lavorata',MESE_SIMULAZIONE=mese_simulazione).exclude(ID=id_simulazione).values())
    return JsonResponse({'context': lista_simulazioni_da_confrontare})  



def get_province(request):
    regione = request.GET.get('regione')
    if regione:
        lista_province = list(table_cap_prov_reg.objects.filter(REGIONE=regione).values_list('PROVINCIA', flat=True).distinct().order_by('PROVINCIA'))
        return JsonResponse(lista_province, safe=False)
    return JsonResponse([], safe=False)


def get_mesi_distinct():
    '''
    Recuperiamo la lista dei mesi che l'utente può scegliere fornendo il formato mostrato nel seguente esempio: [('2026-02', 'Febbraio 2026'), ('2026-03', 'Marzo 2026')]
    Nota: chiaramente, evitiamo le date passate
    '''
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT DISTINCT TO_CHAR("DELIVERY_DATE", 'YYYY-MM') as anno_mese
            FROM public."SENDER_LIMIT"
            ORDER BY anno_mese
        """)
        lista_mesi = []
        for row in cursor.fetchall():
            data_formattata = datetime.strptime(row[0], '%Y-%m').date().strftime("%B %Y").capitalize()
            lista_mesi.append((row[0],data_formattata))
        return lista_mesi #formato di esempio: [('2026-02', 'Febbraio 2026'), ('2026-03', 'Marzo 2026'), ('2026-04', 'Aprile 2026'), ('2026-05', 'Maggio 2026')]
    

# ERROR PAGES
def handle_error_400(request, exception):
    return render(request, 'error_pages/error_400.html')
def handle_error_403(request, exception):
    return render(request, 'error_pages/error_403.html')
def handle_error_404(request, exception):
    return render(request, 'error_pages/error_404.html')
def handle_error_500(request, *args, **argv):
    return render(request, "error_pages/error_500.html", status=500)