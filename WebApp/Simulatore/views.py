from django.shortcuts import render, redirect
import json
from PagoPA.settings import *
from datetime import date, datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from .models import *
from django.db.models import Q
from django.http import JsonResponse
from django.db import connection
from zoneinfo import ZoneInfo
import boto3
from botocore.config import Config
from django.utils.http import url_has_allowed_host_and_scheme
import csv
from django.http import HttpResponse
from django.views.decorators.gzip import gzip_page
import locale
locale.setlocale(locale.LC_ALL, 'it_IT.UTF-8')


def homepage(request):
    """
    Homepage che coincide con la pagina di riepilogo delle simulazioni effettuate
    """
    lista_simulazioni = table_simulazione.objects.exclude(STATO='Bozza').order_by('-TIMESTAMP_ESECUZIONE')
    # recupero la lista degli id simulazione che hanno capacità simulate per CAP -> serve per capire su quali simulazioni mostrare il button download capacità per CAP 
    lista_idsimulazione_capacita_cap_disponibili = table_capacita_simulate_cap.objects.all().values_list('SIMULAZIONE_ID', flat=True).distinct()
    
    for singola_simulazione in lista_simulazioni:
        # cambio stato su 'Non completata' se siamo sullo stato 'In lavorazione' da più di 2gg
        if singola_simulazione.STATO=='In lavorazione' and singola_simulazione.TIMESTAMP_ESECUZIONE < (datetime.now(ZoneInfo("Europe/Rome")).replace(tzinfo=None) - timedelta(days=2)):
            singola_simulazione.STATO = 'Non completata'
        # cambio stato su 'In lavorazione' per schedulata con timestamp_esecuzione <= now()
        if singola_simulazione.STATO=='Schedulata' and singola_simulazione.TRIGGER=='Schedule' and singola_simulazione.TIMESTAMP_ESECUZIONE <= datetime.now(ZoneInfo("Europe/Rome")).replace(tzinfo=None):
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
        'lista_simulazioni': lista_simulazioni,
        'lista_idsimulazione_capacita_cap_disponibili': lista_idsimulazione_capacita_cap_disponibili
    }
    return render(request, "home.html", context)


def risultati(request, id_simulazione):
    """
    Pagina gestita utilizzando DASH che mostra i risultati di una simulazione
    """
    return render(request, "Simulatore/dashboard_risultati.html")

def confronto_risultati(request, id1, id2):
    """
    Pagina gestita utilizzando DASH che mostra i risultati a confronto di due simulazioni
    """
    return render(request, "Simulatore/dashboard_confronto_risultati.html")


def calendario(request):
    """
    Pagina che mostra il calendario delle simulazioni
    """
    return render(request, "calendario/calendario.html")

def bozze(request):
    """
    Pagina che mostra le simulazioni in uno stato di bozza
    """
    lista_bozze = table_simulazione.objects.filter(STATO='Bozza').order_by('-TIMESTAMP_ESECUZIONE')
    context = {
        'lista_bozze': lista_bozze
    }
    return render(request, "simulazioni/bozze.html", context)

def nuova_simulazione(request, id_simulazione):
    """
    Pagina che permette all'utente di inserire una nuova simulazione
    """
    # Mese da simulare
    lista_mesi = recupero_lista_mesi_simulazione_univoci()

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
    """
    Pagina di salvataggio di una simulazione
    """
    last_update_timestamp = datetime.now(ZoneInfo("Europe/Rome")).strftime('%Y-%m-%d %H:%M:%S')
    tipo_simulazione = 'Manuale'
    # recupero parametri dalla pagina html
    nome_simulazione,descrizione_simulazione,timestamp_esecuzione,tipo_trigger,stato,mese_da_simulare,tipo_capacita_da_modificare,capacita_json = recupero_parametri_input_utente(request)

    # salvataggio simulazione sul db (tabella SIMULAZIONE)
    if request.POST['id_simulazione'] == '' or 'id_simulazione' not in request.POST or request.POST['new_from_old']=='True': # la prima condizione si verifica con il salva_bozza, la seconda condizione si verifica con avvia scheduling, la terza con new_from_old
        # nuova simulazione o new_from_old
        id_simulazione_salvata = salvataggio_db_nuova_simulazione(nome_simulazione,descrizione_simulazione,stato,tipo_trigger,timestamp_esecuzione,mese_da_simulare,tipo_capacita_da_modificare,tipo_simulazione)
    else:
        # simulazione esistente che viene modificata
        id_simulazione_salvata = aggiornamento_db_simulazione_esistente(request.POST['id_simulazione'],nome_simulazione,descrizione_simulazione,stato,tipo_trigger,timestamp_esecuzione,mese_da_simulare,tipo_capacita_da_modificare,tipo_simulazione)

    # salvataggio sul db capacità modificate dall'utente (tabella CAPACITÀ SIMULATE)
    if mese_da_simulare != None and tipo_capacita_da_modificare != None:
        lista_all_capacita_modificate = table_capacita_simulate.objects.filter(SIMULAZIONE_ID = id_simulazione_salvata)
        lista_old_capacita_modificate = lista_all_capacita_modificate.exclude(ACTIVATION_DATE_TO__isnull=True)
        if lista_old_capacita_modificate:
            # ci sono già capacità sul db, dobbiamo effettuare upsert
            upsert_capacita_db(lista_all_capacita_modificate,lista_old_capacita_modificate,tipo_capacita_da_modificare,capacita_json,last_update_timestamp,id_simulazione_salvata)
        else:
            # non ci sono capacità nella tabella CAPACITA_SIMULATE, dobbiamo effettuare l'insert perché ci troviamo in una nuova simulazione
            inserimento_nuove_capacita_db(tipo_capacita_da_modificare,capacita_json,last_update_timestamp,id_simulazione_salvata)
        # aggiunta prodotti RS nella tabella CAPACITA_SIMULATE recuperandoli dalla tabella DECLARED_CAPACITY
        gestione_prodotti_rs(mese_da_simulare,tipo_capacita_da_modificare,last_update_timestamp,id_simulazione_salvata)
        
        
    # gestiamo il redirect dopo aver salvato la simulazione
    if request.POST['stato'] == 'Bozza':
        return redirect("bozze")
    elif request.POST['stato'] == 'Schedulata-Inlavorazione':
        return redirect("home")

def rimuovi_simulazione(request, id_simulazione):
    """
    Rimuove una simulazione esistente a partire dall'id_simulazione
    """
    # rimozione trigger eventbridge scheduler presente
    rimuovi_trigger_eventbridge_scheduler(id_simulazione)
    # il try-catch serve per evitare che .get non trovi nulla dando errore
    try:
        simulazione_da_rimuovere = table_simulazione.objects.get(ID=id_simulazione)
        table_capacita_simulate.objects.filter(SIMULAZIONE_ID=simulazione_da_rimuovere.ID).delete()
        simulazione_da_rimuovere.delete()
    except:
        pass

    next_url = request.GET.get('next')

    if next_url and url_has_allowed_host_and_scheme(
        next_url,
        allowed_hosts={request.get_host()},
        require_https=request.is_secure(),
    ):
        return redirect(next_url)

    return redirect('/')


# AJAX
def ajax_recupero_capacita(request):
    """
    Questa funzione viene invocata tramite tecnologia AJAX e ci permette di recuperare le capacità

    Args:
        request (django.core.handlers.wsgi.WSGIRequest): oggetto creato da Django a partire dalla richiesta HTTP grezza che arriva dal server web e contiene l'input dell'utente
        
    Returns:
        bool: indica se stiamo recuperando le capacità per una nuova simulazione o vogliamo recuperare quelle di una simulazione esistente
        list of dict: contiene gli elementi da disporre nelle tabelle nello STEP 3 della pagina 'nuova_simulazione'
        string: BAU, Picco, Combinata, Produzione
    """
    # recuperiamo gli input dell'utente nello step 1 della nuova simulazione
    mese_da_simulare = request.GET['mese_da_simulare_selezionato']
    tipo_capacita_selezionata = request.GET['tipo_capacita_selezionata']
    id_simulazione = request.GET['id_simulazione']
    get_modified_capacity = request.GET['get_modified_capacity']
    # calcolo del primo lunedì del mese successivo al mese selezionato dall'utente per la simulazione
    anno, mese = map(int, mese_da_simulare.split('-'))
    primo_lunedi_mese_successivo = date(anno, mese, 1) + relativedelta(months=+1)
    offset = (0 - primo_lunedi_mese_successivo.weekday()) % 7 # RICORDA: con weekday(), 0=lunedì, 6=domenica
    primo_lunedi_mese_successivo = str(primo_lunedi_mese_successivo + timedelta(days=offset))
    if request.accepts:
        if id_simulazione == '' or get_modified_capacity=='false':
            # NUOVA SIMULAZIONE
            lista_capacita_grezze = list(view_output_capacity_setting.objects.filter(MONTH_DELIVERY=mese).filter(Q(ACTIVATION_DATE_FROM__year=mese_da_simulare.split('-')[0], ACTIVATION_DATE_FROM__month=mese_da_simulare.split('-')[1]) | Q(ACTIVATION_DATE_FROM__year=primo_lunedi_mese_successivo.split('-')[0], ACTIVATION_DATE_FROM__month=primo_lunedi_mese_successivo.split('-')[1], ACTIVATION_DATE_FROM__day=primo_lunedi_mese_successivo.split('-')[2])).order_by('UNIFIED_DELIVERY_DRIVER','REGIONE','PROVINCIA','ACTIVATION_DATE_FROM').values())
            nuova_simulazione = True
        else:
            # RECUPERIAMO LE CAPACITÀ DA UNA SIMULAZIONE ESISTENTE (per modifica simulazione, modifica bozza o nuova simulazione partendo dallo stesso input). Nota: escludiamo gli ACTIVATION_DATE_TO nulli inseriti nella prima fase di creazione della simulazione per capacità con ACTIVATION_DATE_TO NULL. Escludiamo anche PRODUCT_890 e PRODUCT_AR nulli perché riguardano le capacità recuperate a valle del run e capita nel caso di new_simulazione_from_old
            lista_capacita_grezze = list(view_output_modified_capacity_setting.objects.filter(SIMULAZIONE_ID = id_simulazione).exclude(ACTIVATION_DATE_TO__isnull=True).exclude(PRODUCT_890__isnull=True).exclude(PRODUCT_AR__isnull=True).order_by('UNIFIED_DELIVERY_DRIVER','REGIONE','PROVINCIA','ACTIVATION_DATE_FROM').values())
            nuova_simulazione = False
        lista_output_capacity_setting = {}
        for item in lista_capacita_grezze:
            recapitista = item['UNIFIED_DELIVERY_DRIVER']
            regione = item['REGIONE']
            cod_sigla_provincia = item['COD_SIGLA_PROVINCIA']
            # PRODUCT TYPE: boolean, valori True/False
            product = ''
            if item['PRODUCT_890']:
                product += '890-'
            if item['PRODUCT_AR']:
                product += 'AR-'
            if product!='':
                product = product[:-1]
            if recapitista not in lista_output_capacity_setting:
                lista_output_capacity_setting[recapitista] = {}
            if regione+'_'+cod_sigla_provincia+'_'+product not in lista_output_capacity_setting[recapitista]:
                lista_output_capacity_setting[recapitista][regione+'_'+cod_sigla_provincia+'_'+product] = []
            
            provincia = item['PROVINCIA']
            post_monthly_estimate = item['SUM_MONTHLY_ESTIMATE']
            if item['PRODUCTION_CAPACITY'] != None:
                production_capacity = item['PRODUCTION_CAPACITY']
            else:
                production_capacity = 0
            peak_capacity = item['PEAK_CAPACITY']
            if nuova_simulazione:
                capacity = item['CAPACITY']
                original_capacity = capacity
                # qui è solo fittizia, la andremo a calcolare successivamente per le nuove simulazioni
                post_weekly_estimate = None
            else:
                original_capacity = item['CAPACITY']
                capacity = item['MODIFIED_CAPACITY']
                post_weekly_estimate = item['SUM_WEEKLY_ESTIMATE']
            activation_date_from = item['ACTIVATION_DATE_FROM']
            # se il primo lunedì del mese è il giorno 1 non la consideriamo come prima settimana. Questa cosa va fatta solamente per la prima settimana del mese corrente e non la prima del mese successivo
            if activation_date_from.day == 1 and activation_date_from.month == mese:
                continue
            activation_date_to = item['ACTIVATION_DATE_TO']
            
            lista_output_capacity_setting[recapitista][regione+'_'+cod_sigla_provincia+'_'+product].append(
                {
                    'regione': regione,
                    'provincia': provincia,
                    'cod_sigla_provincia': cod_sigla_provincia,
                    'post_monthly_estimate': post_monthly_estimate,
                    'post_weekly_estimate': post_weekly_estimate,
                    'product': product,
                    'activation_date_from': activation_date_from,
                    'activation_date_to': activation_date_to,
                    'capacity': capacity,
                    'peak_capacity': peak_capacity,
                    'production_capacity': production_capacity,
                    'original_capacity': original_capacity
                }
            )
        # calcoliamo post_weekly_estimate come post_monthly_estimate distribuita sul numero di settimane per ogni recapitista-regione-provincia-prodotto
        for recapitista,dizionario_reg_prov_prod in lista_output_capacity_setting.items():
            for righe_tabella in dizionario_reg_prov_prod.values():
                for singola_riga in righe_tabella:
                    if nuova_simulazione:
                        # calcoliamo il numero di postalizzazioni settimanali come postalizzazioni mensili fratto il numero di settimane nel mese
                        singola_riga['post_weekly_estimate'] = int(round(singola_riga['post_monthly_estimate'] / len(righe_tabella), 0))

    return JsonResponse({'nuova_simulazione':nuova_simulazione, 'lista_output_capacity_setting': lista_output_capacity_setting, 'tipo_capacita_selezionata': tipo_capacita_selezionata})



def ajax_recupero_simulazioni_da_confrontare(request):
    """
    Questa funzione viene invocata tramite tecnologia AJAX e ci permette di recuperare le simulazioni da confrontare a partire da un id_simulazione ed un mese_simulazione 

    Args:
        request (django.core.handlers.wsgi.WSGIRequest): oggetto creato da Django a partire dalla richiesta HTTP grezza che arriva dal server web e contiene l'input dell'utente
        
    Returns:
        list: simulazioni confrontabili
    """
    id_simulazione = request.GET['id_simulazione']
    mese_simulazione = request.GET['mese_simulazione']
    lista_simulazioni_da_confrontare = list(table_simulazione.objects.filter(STATO='Lavorata',MESE_SIMULAZIONE=mese_simulazione).exclude(ID=id_simulazione).values())
    return JsonResponse({'context': lista_simulazioni_da_confrontare})  



def recupero_province(request):
    """
    Questa funzione recupera le province a partire da una regione selezionata dall'utente

    Args:
        request (django.core.handlers.wsgi.WSGIRequest): oggetto creato da Django a partire dalla richiesta HTTP grezza che arriva dal server web e contiene l'input dell'utente
        
    Returns:
        list: province recuperate a partire da una regione selezionata
    """
    regione = request.GET.get('regione')
    if regione:
        lista_province = list(table_cap_prov_reg.objects.filter(REGIONE=regione).values_list('PROVINCIA', flat=True).distinct().order_by('PROVINCIA'))
        return JsonResponse(lista_province, safe=False)
    return JsonResponse([], safe=False)


def recupero_lista_mesi_simulazione_univoci():
    """
    Questa funzione recupera la lista dei mesi univoci che l'utente può scegliere per creare una nuova simulazione

    Returns:
        list of tuple: mesi univoci dove il primo elmento della tupla è nel formato YYYY-MM mentre il secondo elemento della tupla contiene il mese scritto per esteso e l'anno in formato YYYY
    """
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
        return lista_mesi #esempio lista_mesi: [('2026-02', 'Febbraio 2026'), ('2026-03', 'Marzo 2026'), ('2026-04', 'Aprile 2026'), ('2026-05', 'Maggio 2026')]



def calcolo_prima_settimana(data_string):
    """
    Questa funzione calcola il primo lunedì del mese corrente da considerare per la simulazione
    
    Args:
        data_string (string): anno-mese (formato YYYY-MM) dal quale partire per calcolare la prima settimana

    Returns:
        string: data del primo lunedì del mese da considerare, formato YYYY-MM-DD
    """
    # from string to datetime
    dt = datetime.strptime(data_string, "%Y-%m")
    # prendiamo il primo giorno del mese
    first_day = datetime(dt.year, dt.month, 1)
    # giorno della settimana (lunedì=0, ... domenica=6)
    offset = (0 - first_day.weekday()) % 7
    # ATTENZIONE: se il primo giorno del mese è lunedì prendiamo come prima settimana utile il secondo lunedì del mese
    if first_day.weekday()==0:
        offset += 7
    first_monday = first_day + timedelta(days=offset)
    return str(first_monday.date())



def crea_trigger_eventbridge_scheduler(id_simulazione, mese_da_simulare, tipo_trigger, timestamp_esecuzione):
    """
    Questa funzione crea un nuovo trigger eventbridge scheduler su AWS
    
    Args:
        id_simulazione (string): identificativo univoco della simulazione di riferimento
    """
    settimana_del_mese_simulazione = calcolo_prima_settimana(mese_da_simulare)
    config = Config(retries={'mode': 'standard', 'max_attempts': 10})
    client = boto3.client("scheduler", region_name="eu-south-1", config=config)
    # parametri da passare alla step function
    payload = {
        "mese_simulazione": settimana_del_mese_simulazione, # formato YYYY-MM-DD
        "id_simulazione_manuale": str(id_simulazione),
        "tipo_simulazione": "Manuale"
    }
    if tipo_trigger=='Now':
        schedule_time = (datetime.now(ZoneInfo("Europe/Rome")) + timedelta(minutes=2)).astimezone(timezone.utc).replace(tzinfo=None).replace(microsecond=0).isoformat()
    else:
        schedule_time = timestamp_esecuzione.astimezone(timezone.utc).replace(tzinfo=None).replace(microsecond=0).isoformat()
    schedule_name = f"pn-simulatore-recapiti-SimulazioneManualeId{id_simulazione}"
    response = client.create_schedule(
        Name=schedule_name,
        ScheduleExpression=f"at({schedule_time})",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={
            "Arn": STEP_FUNCTION_ARN,
            "RoleArn": ROLE_EVENTBRIDGE_STARTEXECUTIONSF_ARN,
            "Input": json.dumps(payload),
        },
        ActionAfterCompletion="DELETE"
    ) 


def modifica_trigger_eventbridge_scheduler(id_simulazione, mese_da_simulare, tipo_trigger, timestamp_esecuzione):
    """
    Questa funzione modifica un trigger eventbridge scheduler esistente su AWS
    
    Args:
        id_simulazione (string): identificativo univoco della simulazione di riferimento
    """
    settimana_del_mese_simulazione = calcolo_prima_settimana(mese_da_simulare)
    config = Config(retries={'mode': 'standard', 'max_attempts': 10})
    client = boto3.client("scheduler", region_name="eu-south-1", config=config)
    # parametri da passare alla step function
    payload = {
        "mese_simulazione": settimana_del_mese_simulazione, # formato YYYY-MM-DD
        "id_simulazione_manuale": str(id_simulazione),
        "tipo_simulazione": "Manuale"
    }
    if tipo_trigger=='Now':
        schedule_time = (datetime.now(ZoneInfo("Europe/Rome")) + timedelta(minutes=2)).astimezone(timezone.utc).replace(tzinfo=None).replace(microsecond=0).isoformat()
    else:
        schedule_time = timestamp_esecuzione.astimezone(timezone.utc).replace(tzinfo=None).replace(microsecond=0).isoformat()
    schedule_name = f"pn-simulatore-recapiti-SimulazioneManualeId{id_simulazione}"
    response = client.update_schedule(
        Name=schedule_name,
        ScheduleExpression=f"at({schedule_time})",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={
            "Arn": STEP_FUNCTION_ARN,
            "RoleArn": ROLE_EVENTBRIDGE_STARTEXECUTIONSF_ARN,
            "Input": json.dumps(payload),
        },
        ActionAfterCompletion="DELETE"
    )

def rimuovi_trigger_eventbridge_scheduler(id_simulazione):
    """
    Questa funzione rimuove il trigger eventbridge scheduler su AWS
    
    Args:
        id_simulazione (string): identificativo univoco della simulazione di riferimento
    """
    config = Config(retries={'mode': 'standard', 'max_attempts': 10})
    client = boto3.client("scheduler", region_name="eu-south-1", config=config)
    try:
        schedule_name = f"pn-simulatore-recapiti-SimulazioneManualeId{id_simulazione}"
        client.delete_schedule(Name=schedule_name)
    except:
        pass

def recupero_parametri_input_utente(request):
    """
    Questa funzione gestisce il recupero dei parametri specificati dall'utente
    
    Args:
        request (django.core.handlers.wsgi.WSGIRequest): oggetto creato da Django a partire dalla richiesta HTTP grezza che arriva dal server web e contiene l'input dell'utente

    Returns:
        string: nome della simulazione inserito dall'utente
        string: descrizione della simulazione inserita dall'utente
        string: datetime now nel formato YYYY-MM-DD HH:mm:ss
        string: schedule o now
        string: Bozza o Schedulata-Inlavorazione
        string: mese della simulazione scelto dall'utente, formato YYYY-MM
        string: BAU, Picco o Combinata
        dict: capacità inserite in input dall'utente con relative informazioni (regione,cod_sigla_provincia,product,postalizzazioni_mensili,postalizzazioni_settimanali,inizioPeriodoValidita,finePeriodoValidita,capacita_reale,flag_default,capacita_bau_originale)
    """
    nome_simulazione = request.POST['nome_simulazione']
    descrizione_simulazione = None
    if 'descrizione_simulazione' in request.POST:
        descrizione_simulazione = request.POST['descrizione_simulazione']
    if 'inlineRadioOptions' in request.POST:
        if request.POST['inlineRadioOptions'] == 'now':
            timestamp_esecuzione = datetime.now(ZoneInfo("Europe/Rome")).strftime('%Y-%m-%d %H:%M:%S')
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
    return nome_simulazione,descrizione_simulazione,timestamp_esecuzione,tipo_trigger,stato,mese_da_simulare,tipo_capacita_da_modificare,capacita_json

def salvataggio_db_nuova_simulazione(nome_simulazione,descrizione_simulazione,stato,tipo_trigger,timestamp_esecuzione,mese_da_simulare,tipo_capacita_da_modificare,tipo_simulazione):
    """
    Questa funzione gestisce il caso di inserimento sul db di una nuova simulazione
    
    Args:
        nome_simulazione (string): nome della simulazione da salvare sul db
        descrizione_simulazione (string): descrizione della simulazione da salvare sul db
        stato (string): stato della simulazione da salvare sul db
        tipo_trigger (string): tipo trigger della simulazione da salvare sul db
        timestamp_esecuzione (string): datetime now nel formato YYYY-MM-DD HH:mm:ss
        mese_da_simulare (string): mese della simulazione da salvare sul db, formato YYYY-MM
        tipo_capacita_da_modificare (string): tipo capacità modificata della simulazione da salvare sul db
        tipo_simulazione (string): tipo trigger della simulazione da salvare sul db

    Returns:
        int: identificativo univoco della simulazione salvata
    """
    # salvataggio nuova simulazione sul DB
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
    if stato != 'Bozza':
        # try-except inserito perché potrebbero esserci errori durante la creazione del trigger eventbridge scheduler che non dipendono dalla webapp
        try:
            # creare nuovo trigger evendbridge scheduler one-shot che avvia la Step Function
            crea_trigger_eventbridge_scheduler(id_simulazione_salvata.ID, mese_da_simulare, tipo_trigger, timestamp_esecuzione)
        except:
            # eliminiamo il record appena inserito
            id_simulazione_salvata.delete()
            # ricreiamo l'eccezione originale triggerata nel try
            raise
    return id_simulazione_salvata


def aggiornamento_db_simulazione_esistente(id_simulazione,nome_simulazione,descrizione_simulazione,stato,tipo_trigger,timestamp_esecuzione,mese_da_simulare,tipo_capacita_da_modificare,tipo_simulazione):
    """
    Questa funzione gestisce il caso di aggiornamento sul db di una simulazione esistente
    
    Args:
        id_simulazione (int): identificativo univoco della simulazione da modificare
        nome_simulazione (string): nome della simulazione aggiornato da salvare sul db
        descrizione_simulazione (string): descrizione della simulazione aggiornato da salvare sul db
        stato (string): stato della simulazione aggiornato da salvare sul db
        tipo_trigger (string): tipo trigger della simulazione aggiornato da salvare sul db
        timestamp_esecuzione (string): datetime now nel formato YYYY-MM-DD HH:mm:ss
        mese_da_simulare (string): mese della simulazione aggiornato da salvare sul db, formato YYYY-MM
        tipo_capacita_da_modificare (string): tipo capacità modificata della simulazione aggiornato da salvare sul db
        tipo_simulazione (string): tipo trigger della simulazione aggiornato da salvare sul db

    Returns:
        int: identificativo univoco della simulazione modificata
    """
    # modifica simulazione sul DB
    simulazione_da_modificare = table_simulazione.objects.get(ID = id_simulazione)
    stato_precedente = simulazione_da_modificare.STATO
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
    if stato_precedente == 'Schedulata':
        if stato == 'Schedulata':
            # modificare trigger evendbridge scheduler one-shot esistente che avvia la Step Function
            modifica_trigger_eventbridge_scheduler(id_simulazione_salvata.ID, mese_da_simulare, tipo_trigger, timestamp_esecuzione)
        elif stato == 'Bozza':
            # rimozione trigger eventbridge scheduler presente
            rimuovi_trigger_eventbridge_scheduler(id_simulazione_salvata.ID)
    elif stato_precedente == 'Bozza':
        if stato == 'Schedulata' or stato == 'In lavorazione':
            # creare nuovo trigger evendbridge scheduler one-shot che avvia la Step Function
            crea_trigger_eventbridge_scheduler(id_simulazione_salvata.ID, mese_da_simulare, tipo_trigger, timestamp_esecuzione)
    return id_simulazione_salvata


def upsert_capacita_db(lista_all_capacita_modificate,lista_old_capacita_modificate,tipo_capacita_da_modificare,capacita_json,last_update_timestamp,id_simulazione_salvata):
    """
    Questa funzione aggiorna sul db le capacità scelte dall'utente
    
    Args:
        lista_all_capacita_modificate (list): lista capacità già presenti sul db, recuperate dalla tabella del db denominata 'CAPACITA_SIMULATE'
        lista_old_capacita_modificate (list): lista_all_capacita_modificate escluse le capacità con ACTIVATION_DATE_TO settata a NULL
        tipo_capacita_da_modificare (string): BAU, Picco o Combinata
        capacita_json (dict): capacità inserite in input dall'utente con relative informazioni (regione,cod_sigla_provincia,product,postalizzazioni_mensili,postalizzazioni_settimanali,inizioPeriodoValidita,finePeriodoValidita,capacita_reale,flag_default,capacita_bau_originale)
        last_update_timestamp (string): datetime now nel formato YYYY-MM-DD HH:mm:ss
        id_simulazione_salvata (int): identificativo univoco della simulazione di riferimento
    """
    lista_all_capacita_modificate.filter(ACTIVATION_DATE_TO__isnull=True).delete() # rimuoviamo vecchie capacità con ACTIVATION_DATE_TO NULL
    lista_nuove_capacita_da_salvare = [] # nuove capacità con ACTIVATION_DATE_TO NULL + eventuali capacità aggiuntive
    lookup = {}
    # existing_keys -> lista delle triplette recapitista,provincia,date_from già presenti
    existing_keys = set()
    for recapitista, righe_tabella in capacita_json.items():
        # e.g. righe_tabella: [{'regione': 'Abruzzo', 'cod_sigla_provincia': 'AQ', 'product': '890-AR', 'postalizzazioni_mensili': '460', 'postalizzazioni_settimanali': '153', 'inizioPeriodoValidita': '20/10/2025', 'finePeriodoValidita': '26/10/2025', 'capacita': '50', 'capacita_reale': '1500'}, {'regione': 'Abruzzo', 'cod_sigla_provincia': 'AQ', 'product': '890-AR', 'postalizzazioni_mensili': '460', 'postalizzazioni_settimanali': '153', 'inizioPeriodoValidita': '27/10/2025', 'finePeriodoValidita': '02/11/2025', 'capacita': '1500', 'capacita_reale': '1500'},...]
        # inizializzazione variabili che tengono conto dell'iterazione precedente per le capacità con ACTIVATION_DATE_TO NULL 
        recapregioneprovincia_precedente = None
        capacita_reale_precedente_prima_settimana = None
        capacita_reale_attuale_prima_settimana = None
        postalizzazioni_mensili_precedente = None
        postalizzazioni_settimanali_precedente = None
        activation_date_from_precedente = None
        product_890_precedente = None
        product_ar_precedente = None
        for row in righe_tabella:
            key = (recapitista, row["cod_sigla_provincia"], datetime.strptime(row['inizioPeriodoValidita']+' 00:00:00', '%d/%m/%Y %H:%M:%S'))
            lookup[key] = (row['finePeriodoValidita'],row['postalizzazioni_mensili'],row['regione'],row['product'],row['capacita'],row['capacita_reale'],row['flag_default'])
            # aggiornamento dati iterazione precedente e corrente
            recapregioneprovincia_corrente = recapitista+'__'+row['regione']+'__'+row['cod_sigla_provincia']
            postalizzazioni_mensili_corrente = row['postalizzazioni_mensili']
            postalizzazioni_settimanali_corrente = row['postalizzazioni_settimanali']
            activation_date_from_corrente = row['inizioPeriodoValidita']
            product_890_corrente = True if '890' in row['product'] else False
            product_ar_corrente = True if 'AR' in row['product'] else False
            # cattura capacità reale prima settimana di recapitista-recione-provincia
            if recapregioneprovincia_precedente != recapregioneprovincia_corrente:
                capacita_reale_precedente_prima_settimana = capacita_reale_attuale_prima_settimana
                # per tipo capacita "Combinata" dobbiamo impostare come capacità quella di BAU (di una delle settimane del mese selezionato, dato che sono uguali) per la capacità dopo l'ultima settimana (ACTIVATION_DATE_TO = None)
                if tipo_capacita_da_modificare == 'Combinata':
                    capacita_reale_attuale_prima_settimana = row['capacita_bau_originale']
                else:
                    capacita_reale_attuale_prima_settimana = row['capacita_reale']
            # capacità con ACTIVATION_DATE_TO NULL da aggiungere ad ogni recapitista-regione-provincia a partire dalla settimana successiva all'ultima specificata dall'utente
            if recapregioneprovincia_precedente != recapregioneprovincia_corrente and recapregioneprovincia_precedente != None:
                lista_nuove_capacita_da_salvare.append(
                    table_capacita_simulate(
                        UNIFIED_DELIVERY_DRIVER = recapregioneprovincia_precedente.split('__')[0],
                        ACTIVATION_DATE_FROM = datetime.strptime(activation_date_from_precedente+' 00:00:00', "%d/%m/%Y %H:%M:%S") + timedelta(days=7),
                        ACTIVATION_DATE_TO = None,
                        CAPACITY = capacita_reale_precedente_prima_settimana,
                        SUM_MONTHLY_ESTIMATE = postalizzazioni_mensili_precedente,
                        SUM_WEEKLY_ESTIMATE = postalizzazioni_settimanali_precedente,
                        REGIONE = recapregioneprovincia_precedente.split('__')[1],
                        COD_SIGLA_PROVINCIA = recapregioneprovincia_precedente.split('__')[2],
                        PRODUCT_890 = product_890_precedente,
                        PRODUCT_AR = product_ar_precedente,
                        LAST_UPDATE_TIMESTAMP = last_update_timestamp,
                        FLAG_DEFAULT = False if tipo_capacita_da_modificare == 'Picco' else True,
                        SIMULAZIONE_ID = id_simulazione_salvata
                    )
                )
            # aggiornamento dati iterazione precedente e corrente
            recapregioneprovincia_precedente = recapregioneprovincia_corrente
            postalizzazioni_mensili_precedente = postalizzazioni_mensili_corrente
            postalizzazioni_settimanali_precedente = postalizzazioni_settimanali_corrente
            activation_date_from_precedente = activation_date_from_corrente
            product_890_precedente = product_890_corrente
            product_ar_precedente = product_ar_corrente

        # capacità con ACTIVATION_DATE_TO NULL -> ultima riga prima di cambiare recapitista
        lista_nuove_capacita_da_salvare.append(
            table_capacita_simulate(
                UNIFIED_DELIVERY_DRIVER = recapitista,
                ACTIVATION_DATE_FROM = datetime.strptime(row['inizioPeriodoValidita']+' 00:00:00', "%d/%m/%Y %H:%M:%S") + timedelta(days=7),
                ACTIVATION_DATE_TO = None,
                CAPACITY = capacita_reale_attuale_prima_settimana,
                SUM_MONTHLY_ESTIMATE = row['postalizzazioni_mensili'],
                SUM_WEEKLY_ESTIMATE = row['postalizzazioni_settimanali'],
                REGIONE = row['regione'],
                COD_SIGLA_PROVINCIA = row['cod_sigla_provincia'],
                PRODUCT_890 = True if '890' in row['product'] else False,
                PRODUCT_AR = True if 'AR' in row['product'] else False,
                LAST_UPDATE_TIMESTAMP = last_update_timestamp,
                FLAG_DEFAULT = False if tipo_capacita_da_modificare == 'Picco' else True,
                SIMULAZIONE_ID = id_simulazione_salvata
            )
        )

    for singola_capacita in lista_old_capacita_modificate:
        key = (singola_capacita.UNIFIED_DELIVERY_DRIVER, singola_capacita.COD_SIGLA_PROVINCIA, singola_capacita.ACTIVATION_DATE_FROM)
        if key in lookup:
            singola_capacita.CAPACITY = lookup[key][4]
            singola_capacita.FLAG_DEFAULT = False if lookup[key][6] == '0' else True
            
        existing_keys.add(key)
    for key,riga in lookup.items():
        # e.g. key: ('Poste', 'FI', datetime.datetime(2025, 11, 3, 0, 0)) -> UNIFIED_DELIVERY_DRIVER, COD_SIGLA_PROVINCIA, ACTIVATION_DATE_FROM
        # e.g. riga: ('09/11/2025', '460', 'Abruzzo', '890-AR', '1000', '1000', 'BAU') -> ACTIVATION_DATE_TO, SUM_MONTHLY_ESTIMATE, REGIONE, PRODOTTI, capacità modificata dall'utente, capacita_reale
        if key not in existing_keys:
            recapitista, cod_sigla_provincia, activation_date_from = key
            lista_nuove_capacita_da_salvare.append(
                table_capacita_simulate(
                    UNIFIED_DELIVERY_DRIVER=recapitista,
                    COD_SIGLA_PROVINCIA=cod_sigla_provincia,
                    ACTIVATION_DATE_FROM=activation_date_from,
                    ACTIVATION_DATE_TO = datetime.strptime(riga[0]+' 23:59:59', '%d/%m/%Y %H:%M:%S'),
                    SUM_MONTHLY_ESTIMATE = riga[1],
                    SUM_WEEKLY_ESTIMATE = 0,
                    REGIONE = riga[2],
                    PRODUCT_890 = True if '890' in riga[3] else False,
                    PRODUCT_AR = True if 'AR' in riga[3] else False,
                    LAST_UPDATE_TIMESTAMP = last_update_timestamp,
                    CAPACITY=riga[4],
                    FLAG_DEFAULT = False if riga[6] == '0' else True,
                    SIMULAZIONE_ID = id_simulazione_salvata
                )
            )
    # aggiorniamo le righe esistenti per i campi "CAPACITY" e "FLAG_DEFAULT"
    table_capacita_simulate.objects.bulk_update(lista_old_capacita_modificate,["CAPACITY","FLAG_DEFAULT"])
    if len(lista_nuove_capacita_da_salvare) != 0:
        # inseriamo eventuali nuove righe
        table_capacita_simulate.objects.bulk_create(lista_nuove_capacita_da_salvare)


def inserimento_nuove_capacita_db(tipo_capacita_da_modificare,capacita_json,last_update_timestamp,id_simulazione_salvata):
    """
    Questa funzione inserisce sul db le capacità scelte dall'utente
    
    Args:
        tipo_capacita_da_modificare (string): BAU, Picco o Combinata
        capacita_json (dict): capacità inserite in input dall'utente con relative informazioni (regione,cod_sigla_provincia,product,postalizzazioni_mensili,postalizzazioni_settimanali,inizioPeriodoValidita,finePeriodoValidita,capacita_reale,flag_default,capacita_bau_originale)
        last_update_timestamp (string): datetime now nel formato YYYY-MM-DD HH:mm:ss
        id_simulazione_salvata (int): identificativo univoco della simulazione di riferimento
    """
    # scrittura sul db nella tabella CAPACITA_SIMULATE
    lista_capacita_da_salvare = []
    for recapitista, righe_tabella in capacita_json.items():
        # e.g. righe_tabella: [{'regione': 'Abruzzo', 'cod_sigla_provincia': 'AQ', 'product': '890-AR', 'postalizzazioni_mensili': '460', 'postalizzazioni_settimanali': '153', 'inizioPeriodoValidita': '20/10/2025', 'finePeriodoValidita': '26/10/2025', 'capacita': '50', 'capacita_reale': '1500'}, {'regione': 'Abruzzo', 'cod_sigla_provincia': 'AQ', 'product': '890-AR', 'postalizzazioni_mensili': '460', 'postalizzazioni_settimanali': '153', 'inizioPeriodoValidita': '27/10/2025', 'finePeriodoValidita': '02/11/2025', 'capacita': '1500', 'capacita_reale': '1500'},...]
        # inizializzazione variabile che tiene conto dell'iterazione precedente per le capacità con ACTIVATION_DATE_TO NULL
        recapregioneprovincia_precedente = None
        capacita_reale_precedente_prima_settimana = None
        capacita_reale_attuale_prima_settimana = None
        for singola_riga in righe_tabella:
            # aggiornamento dati iterazione corrente
            recapregioneprovincia_corrente = recapitista+'__'+singola_riga['regione']+'__'+singola_riga['cod_sigla_provincia']
            # cattura capacità reale prima settimana di recapitista-recione-provincia
            if recapregioneprovincia_precedente != recapregioneprovincia_corrente:
                capacita_reale_precedente_prima_settimana = capacita_reale_attuale_prima_settimana
                # per tipo capacita "Combinata" dobbiamo impostare come capacità quella di BAU (di una delle settimane del mese selezionato, dato che sono uguali) per la capacità dopo l'ultima settimana (ACTIVATION_DATE_TO = None)
                if tipo_capacita_da_modificare == 'Combinata':
                    capacita_reale_attuale_prima_settimana = singola_riga['capacita_bau_originale']
                else:
                    capacita_reale_attuale_prima_settimana = singola_riga['capacita_reale']
            # capacità con ACTIVATION_DATE_TO NULL da aggiungere ad ogni recapitista-regione-provincia a partire dalla settimana successiva all'ultima specificata dall'utente
            if recapregioneprovincia_precedente != recapregioneprovincia_corrente and recapregioneprovincia_precedente != None:
                lista_capacita_da_salvare.append(
                    table_capacita_simulate(
                        UNIFIED_DELIVERY_DRIVER = recapregioneprovincia_precedente.split('__')[0],
                        ACTIVATION_DATE_FROM = lista_capacita_da_salvare[-1].ACTIVATION_DATE_FROM + timedelta(days=7),
                        ACTIVATION_DATE_TO = None,
                        CAPACITY = capacita_reale_precedente_prima_settimana,
                        SUM_MONTHLY_ESTIMATE = lista_capacita_da_salvare[-1].SUM_MONTHLY_ESTIMATE,
                        SUM_WEEKLY_ESTIMATE = lista_capacita_da_salvare[-1].SUM_WEEKLY_ESTIMATE,
                        REGIONE = recapregioneprovincia_precedente.split('__')[1],
                        COD_SIGLA_PROVINCIA = recapregioneprovincia_precedente.split('__')[2],
                        PRODUCT_890 = lista_capacita_da_salvare[-1].PRODUCT_890,
                        PRODUCT_AR = lista_capacita_da_salvare[-1].PRODUCT_AR,
                        LAST_UPDATE_TIMESTAMP = last_update_timestamp,
                        FLAG_DEFAULT = False if tipo_capacita_da_modificare == 'Picco' else True,
                        SIMULAZIONE_ID = id_simulazione_salvata
                    )
                )
            # capacità modificate dall'utente (NON con ACTIVATION_DATE_TO NULL)
            lista_capacita_da_salvare.append(
                table_capacita_simulate(
                    UNIFIED_DELIVERY_DRIVER = recapitista,
                    ACTIVATION_DATE_FROM = datetime.strptime(singola_riga['inizioPeriodoValidita']+' 00:00:00', '%d/%m/%Y %H:%M:%S'),
                    ACTIVATION_DATE_TO = datetime.strptime(singola_riga['finePeriodoValidita']+' 23:59:59', '%d/%m/%Y %H:%M:%S'),
                    CAPACITY = singola_riga['capacita'],
                    SUM_MONTHLY_ESTIMATE = singola_riga['postalizzazioni_mensili'],
                    SUM_WEEKLY_ESTIMATE = singola_riga['postalizzazioni_settimanali'],
                    REGIONE = singola_riga['regione'],
                    COD_SIGLA_PROVINCIA = singola_riga['cod_sigla_provincia'],
                    PRODUCT_890 = True if '890' in singola_riga['product'] else False,
                    PRODUCT_AR = True if 'AR' in singola_riga['product'] else False,
                    LAST_UPDATE_TIMESTAMP = last_update_timestamp,
                    FLAG_DEFAULT = False if singola_riga['flag_default']=='0' else True,
                    SIMULAZIONE_ID = id_simulazione_salvata
                )
            )
            
            # aggiornamento dati iterazione precedente
            recapregioneprovincia_precedente = recapregioneprovincia_corrente
        
        # capacità con ACTIVATION_DATE_TO NULL -> ultima riga prima di cambiare recapitista
        lista_capacita_da_salvare.append(
            table_capacita_simulate(
                UNIFIED_DELIVERY_DRIVER = recapitista,
                ACTIVATION_DATE_FROM = datetime.strptime(singola_riga['inizioPeriodoValidita']+' 00:00:00', "%d/%m/%Y %H:%M:%S") + timedelta(days=7),
                ACTIVATION_DATE_TO = None,
                CAPACITY = capacita_reale_attuale_prima_settimana,
                SUM_MONTHLY_ESTIMATE = singola_riga['postalizzazioni_mensili'],
                SUM_WEEKLY_ESTIMATE = singola_riga['postalizzazioni_settimanali'],
                REGIONE = singola_riga['regione'],
                COD_SIGLA_PROVINCIA = singola_riga['cod_sigla_provincia'],
                PRODUCT_890 = True if '890' in singola_riga['product'] else False,
                PRODUCT_AR = True if 'AR' in singola_riga['product'] else False,
                LAST_UPDATE_TIMESTAMP = last_update_timestamp,
                FLAG_DEFAULT = False if tipo_capacita_da_modificare == 'Picco' else True,
                SIMULAZIONE_ID = id_simulazione_salvata
            )
        )           
    # inserimento sul db delle capacità modificate nella tabella CAPACITA_SIMULATE
    table_capacita_simulate.objects.bulk_create(lista_capacita_da_salvare)


def gestione_prodotti_rs(mese_da_simulare,tipo_capacita_da_modificare,last_update_timestamp,id_simulazione_salvata):
    """
    Questa funzione permette di aggiungere i prodotti RS nella tabella del db denominata 'CAPACITA_SIMULATE' recuperandoli dalla tabella del db denominata 'DECLARED_CAPACITY'

    Args:
        mese_da_simulare (string): mese di riferimento della simulazione, formato YYYY-MM
        tipo_capacita_da_modificare (string): BAU, Picco o Combinata
        last_update_timestamp (string): datetime now nel formato YYYY-MM-DD HH:mm:ss
        id_simulazione_salvata (int): identificativo univoco della simulazione di riferimento
    """
    # recupero record su tabella DECLARED CAPACITY con prodotti RS (non AR e non 890) con ACTIVATION_DATE_FROM uguale al primo lunedì di simulazione
    primo_lunedi_mese_corrente = calcolo_prima_settimana(mese_da_simulare)
    lista_prodotti_rs = table_declared_capacity.objects.filter(PRODUCT_890=False, PRODUCT_AR=False, PRODUCT_RS=True, ACTIVATION_DATE_FROM=primo_lunedi_mese_corrente+' 00:00:00')
    # inserimento su tabella CAPACITA_SIMULATE
    lista_capacita_rs_da_salvare = []
    for singola_capacita_rs in lista_prodotti_rs:
        if tipo_capacita_da_modificare=='BAU' or tipo_capacita_da_modificare=='Combinata':
            capacity_per_rs = singola_capacita_rs.CAPACITY
        else:
            capacity_per_rs = singola_capacita_rs.PEAK_CAPACITY
        lista_capacita_rs_da_salvare.append(
            table_capacita_simulate(
                UNIFIED_DELIVERY_DRIVER = singola_capacita_rs.UNIFIED_DELIVERY_DRIVER,
                ACTIVATION_DATE_FROM = primo_lunedi_mese_corrente+' 00:00:00',
                ACTIVATION_DATE_TO = None,
                CAPACITY = capacity_per_rs,
                SUM_MONTHLY_ESTIMATE = 0,
                SUM_WEEKLY_ESTIMATE = 0,
                REGIONE = None,
                COD_SIGLA_PROVINCIA = singola_capacita_rs.GEOKEY,
                PRODUCT_890 = False,
                PRODUCT_AR = False,
                LAST_UPDATE_TIMESTAMP = last_update_timestamp,
                FLAG_DEFAULT = False if tipo_capacita_da_modificare == 'Picco' else True,
                SIMULAZIONE_ID = id_simulazione_salvata
            )
        )
    table_capacita_simulate.objects.bulk_create(lista_capacita_rs_da_salvare)



@gzip_page # utile per comprimere la risposta
def download_capacita_per_provincia(request, id_simulazione, recupero_capacita_modificate):
    """
    Questa funzione viene chiamata quando l'utente vuole scaricare le capacità per provincia in formato csv. A partire dall'id_simulazione, vengono recuperate le capacità scelte dall'utente e creato il csv

    Args:
        id_simulazione (string): identificativo univoco della simulazione

    Returns:
        django.http.response.HttpResponse: contiene nome file, stream dell'oggett e url per il download
    """
    datetime_now = datetime.now(ZoneInfo("Europe/Rome")).replace(tzinfo=None).strftime("%Y%m%d")
    # creiamo la response con header csv
    response = HttpResponse(content_type='text/csv')
    if recupero_capacita_modificate == 'true':
        response['Content-Disposition'] = f'attachment; filename="CapacitaModificatePerProvincia_id{id_simulazione}.csv"'
    else:
        response['Content-Disposition'] = f'attachment; filename="CapacitaPerProvincia_id{id_simulazione}.csv"'
    # creiamo il writer csv
    writer = csv.writer(response, delimiter=';')
    # header del csv
    writer.writerow(['unifiedDeliveryDriver','geoKey','capacity','peakCapacity','activationDateFrom','activationDateTo','products'])
    # recuperiamo i record dal db
    if recupero_capacita_modificate == 'true':
        lista_capacita = table_capacita_simulate.objects.filter(SIMULAZIONE_ID=id_simulazione, FLAG_DEFAULT=False).values("UNIFIED_DELIVERY_DRIVER","COD_SIGLA_PROVINCIA","CAPACITY","CAPACITY","ACTIVATION_DATE_FROM","ACTIVATION_DATE_TO","PRODUCT_890","PRODUCT_AR")
    else:
        lista_capacita = table_capacita_simulate.objects.filter(SIMULAZIONE_ID=id_simulazione).values("UNIFIED_DELIVERY_DRIVER","COD_SIGLA_PROVINCIA","CAPACITY","CAPACITY","ACTIVATION_DATE_FROM","ACTIVATION_DATE_TO","PRODUCT_890","PRODUCT_AR")
    # scriviamo sul file con chunk_size=1000
    for row in lista_capacita.iterator(chunk_size=1000):
        formtted_row = elaborazione_capacita_per_provincia(row)
        writer.writerow([
            formtted_row['UNIFIED_DELIVERY_DRIVER'],
            formtted_row['COD_SIGLA_PROVINCIA'],
            formtted_row['CAPACITY'],
            formtted_row['CAPACITY'],
            formtted_row['ACTIVATION_DATE_FROM'],
            formtted_row['ACTIVATION_DATE_TO'],
            formtted_row['products']
        ])
    return response

def elaborazione_capacita_per_provincia(row):
    """
    Questa funzione riceve in input un dizionario, elabora alcuni campi (products, ACTIVATION_DATE_FROM e ACTIVATION_DATE_TO) e ritorna il dizionario modificato

    Args:
        row (dict): riga estratta dalla tabella del db denominata

    Returns:
        dict: input al quale sono stati rimossi gli oggetti 'PRODUCT_890' e 'PRODUCT_AR', aggiunto l'oggetto 'products' e modificati gli oggetti 'ACTIVATION_DATE_FROM' e 'ACTIVATION_DATE_TO'
    """
    # formattiamo il product
    product_list = ''
    if row['PRODUCT_890'] == True:
        product_list += '890,'
    if row['PRODUCT_AR'] == True:
        product_list += 'AR,'
    product_list += 'RS'
    # rimuoviamo gli ultimi 2 elementi da ogni riga recuperata dal db (product_890 e product_AR)
    del row['PRODUCT_890']
    del row['PRODUCT_AR']
    # aggiungiamo la stringa creata per il prodotto
    row['products'] = product_list
    # adattiamo la data al formato ISO 8601 in UTC -> YYYY-MM-DDTHH:mm:ss.000Z
    row['ACTIVATION_DATE_FROM'] = row['ACTIVATION_DATE_FROM'].replace(tzinfo=timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    if row['ACTIVATION_DATE_TO']!=None:
        row['ACTIVATION_DATE_TO'] = row['ACTIVATION_DATE_TO'].replace(tzinfo=timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    return row 

def download_capacita_per_cap(request, id_simulazione):
    """
    Questa funzione viene chiamata quando l'utente vuole scaricare le capacità per CAP in formato csv. A partire dall'id_simulazione, viene creato un presigned URL del file csv su S3 che viene messo a disposizione dell'utente per il download

    Args:
        id_simulazione (string): identificativo univoco della simulazione

    Returns:
        dict: presigned url del file csv su S3 + nome del file
    """    
    # creazione istanza client s3
    config = Config(retries={'mode': 'standard', 'max_attempts': 10})
    s3_client = boto3.client("s3", region_name="eu-south-1", endpoint_url="https://s3.eu-south-1.amazonaws.com", config=config)
    print('s3_client creato')
    # recupero simulazione dal db a partire dall'id_simulazione
    simulazione_selezionata = table_simulazione.objects.get(ID = id_simulazione)
    print('simulazione_selezionata recuperata')
    # recupero nome file da scaricare sul bucket s3
    file_key = recupero_filekey_s3(BUCKET_NAME, s3_client, id_simulazione, simulazione_selezionata.TIMESTAMP_ESECUZIONE, simulazione_selezionata.MESE_SIMULAZIONE)
    print('file_key recuperato', file_key)
    filename = f"CapacitaPerCAP_id{id_simulazione}.csv"
    if file_key != 'None':
        # generazione presigned_url per permettere all'utente di scaricare il file
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': BUCKET_NAME,
                'Key': file_key,
                "ResponseContentDisposition": f'attachment; filename={filename}', # inseriamo questa riga per cambiare il nome del file quando l'utente effettua il download
                "ResponseContentType": 'text/csv'
            },
            ExpiresIn=300  # seconds
        )
        print('presigned_url recuperato', presigned_url)
        return presigned_url
    else:
        print('presigned_url non presente')
        return 'None'

def recupero_filekey_s3(bucket_name, s3_client, id_simulazione, timestamp_esecuzione_simulazione, mese_simulazione):
    """
    Recuperiamo la data dell'ultimo recupero dati sottoforma di prefisso del bucket s3 di progetto partendo, per la ricerca della data, dal giorno della simulazione (fino ad massimo di sicurezza di 30 gg precedenti alla data di esecuzione della simulazione)

    Args:
        bucket_name (string): nome del bucket s3 di progetto
        s3_client (botocore.client.S3): connessione ad s3

    Returns:
        string: prefisso del bucket che va dalla cartella 'input/' fino alla cartella contenente i file che verranno successivamente importati tramite l'operazione di IMPORT_DATA
    """

    for _ in range(30):  # limite di sicurezza a 30 gg
        prefix = timestamp_esecuzione_simulazione.strftime("%Y/%m/%d/")
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=f'input/{prefix}{mese_simulazione}/cap_capacities/id_{id_simulazione}/unified/',
            MaxKeys=1
        )
        # se la cartella esiste, ritorno il file key
        if 'Contents' in response:
            print(prefix, prefix)
            return response['Contents'][0]['Key']
        # altrimenti vado al giorno precedente
        timestamp_esecuzione_simulazione -= timedelta(days=1)
    # se non viene trovata alcuna cartella corrispondente
    return 'None'



# ERROR PAGES
def handle_error_400(request, exception):
    return render(request, 'error_pages/error_400.html')
def handle_error_403(request, exception):
    return render(request, 'error_pages/error_403.html')
def handle_error_404(request, exception):
    return render(request, 'error_pages/error_404.html')
def handle_error_500(request, *args, **argv):
    return render(request, "error_pages/error_500.html", status=500)