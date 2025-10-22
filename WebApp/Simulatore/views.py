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

locale.setlocale(locale.LC_ALL, 'it_IT.UTF-8')


def homepage(request):

    lista_simulazioni = table_simulazione.objects.exclude(STATO='Bozza')

    context = {
        'lista_simulazioni': lista_simulazioni
    }
    return render(request, "home.html", context)


def risultati(request, id_simulazione):
    return render(request, "Simulatore/dashboard.html")


def confronto_risultati(request, id_simulazione):
    enti = [
    "Regione Lombardia", "INPS", "AMA SPA", "Comune di Roma", "Regione Lazio",
    "Poste Italiane", "Agenzia Entrate", "Comune di Milano", "Regione Toscana", "Comune di Napoli"
    ]

    # Simulazione dati per 4 settimane
    np.random.seed(42)
    dati = []
    for ente in enti:
        base = np.random.randint(2000, 8000)  # Valore base per ente
        for settimana in range(1, 5):
            # Fluttuazione con rumore
            valore = base + np.random.randint(-2000, 3000)
            valore = max(500, valore)  # Minimo 500
            dati.append([ente, f"Sett. {settimana}", valore])

    # Creazione DataFrame
    df = pd.DataFrame(dati, columns=["Ente", "Settimana", "Postalizzazioni"])
    fig_first = px.line(df, x='Settimana', y='Postalizzazioni', color='Ente', markers=True)

    # istruzione per passare il grafico alla pagina html
    fig_first_for_visualizzation = json.dumps(fig_first, cls=putils.PlotlyJSONEncoder)

    # Definizione di regioni e province associate
    regioni_province = {
        "Lombardia": ["Milano", "Bergamo", "Brescia"],
        "Lazio": ["Roma", "Latina", "Viterbo"],
        "Campania": ["Napoli", "Salerno", "Caserta"],
        "Sicilia": ["Palermo", "Catania", "Messina"]
    }

    # Generazione dataset simulato con andamento decrescente
    dati_regioni = []
    for regione, province in regioni_province.items():
        for provincia in province:
            base = np.random.randint(5000, 12000)
            decremento = np.random.randint(800, 2000)
            for settimana in range(1, 5):
                valore = max(500, base - decremento * (settimana - 1))
                dati_regioni.append([regione, provincia, f"Sett. {settimana}", valore])

    # Creazione DataFrame
    df_regioni = pd.DataFrame(dati_regioni, columns=["Regione", "Provincia", "Settimana", "Postalizzazioni"])
    # Grafico lineare province

    fig_second = px.area(
        df_regioni,
        x="Settimana",
        y="Postalizzazioni",
        color="Provincia",
        line_group="Provincia",
        markers=True,
        title="Previsione Postalizzazioni per Provincia"
    )

    # Dropdown per filtrare per regione
    buttons = []
    for regione in df_regioni["Regione"].unique():
        province_regione = df_regioni[df_regioni["Regione"] == regione]["Provincia"].unique()
        visible = [provincia in province_regione for provincia in df_regioni["Provincia"].unique()]
        buttons.append(
            dict(
                label=regione,
                method="update",
                args=[{"visible": visible}, {"title": f"Previsione Postalizzazioni - {regione}"}]
            )
        )

    # Aggiungi opzione "Tutte"
    buttons.insert(
        0,
        dict(
            label="Tutte",
            method="update",
            args=[{"visible": [True] * df_regioni["Provincia"].nunique()}, {"title": "Previsione Postalizzazioni - Tutte le Regioni"}]
        )
    )

    # Layout con dropdown
    fig_second.update_layout(
        updatemenus=[dict(
            buttons=buttons,
            direction="down",
            showactive=True,
            x=1.15,
            y=1.2
        )]
    )

    # istruzione per passare il grafico alla pagina html
    fig_second_for_visualizzation = json.dumps(fig_second, cls=putils.PlotlyJSONEncoder)

    with open(os.path.join(BASE_DIR, 'static/data/limits_IT_regions.json'), encoding = "utf-8") as f:
        geojson = json.load(f)

    # Definizione recapitisti, regioni e province
    recapitisti = ["Poste", "FSU", "RTI Fulmine", "Post & Service"]
    regioni_province = {
            "Lombardia": ["Milano", "Bergamo", "Brescia"],
            "Lazio": ["Roma", "Latina", "Viterbo"],
            "Campania": ["Napoli", "Salerno", "Caserta"],
            "Sicilia": ["Palermo", "Catania", "Messina"]
        }

    # Creazione dataset
    dati = []
    for recapitista in recapitisti:
        test = np.random.choice([0, 10])
        for regione, province in regioni_province.items():
            test = np.random.choice([0, 10])
            if test >= 2:
                for provincia in province:
                    valore = np.random.choice([0, 1])
                    dati.append([recapitista, regione, provincia, valore])

    df_recapitisti = pd.DataFrame(dati, columns=["Recapitista", "Regione", "Provincia", "Assegnazione"])
    df_recapitisti = df_recapitisti.groupby(['Recapitista','Regione']).agg(
        total_picco=('Assegnazione', 'sum'),
        prov_count=('Provincia', 'count')
    )
    df_recapitisti['prop']= df_recapitisti['total_picco']/df_recapitisti['prov_count']
    # Definisco due soglie (puoi cambiarle come preferisci)
    soglia1 = 0.0001
    soglia2 = 0.5

    # Creo una nuova colonna "fascia"
    def classifica_prop(x):
        if x < soglia1:
            return "No picchi"
        elif x < soglia2:
            return "<50% picchi"
        else:
            return ">=50% picchi"

    df_recapitisti["fascia"] = df_recapitisti["prop"].apply(classifica_prop)

    # Assegno 3 colori fissi
    colori = {
        "No picchi": "green",
        "<50% picchi": "orange",
        ">=50% picchi": "red"
    }

    df_recapitisti = df_recapitisti.reset_index()

    ## mappa testo->numero per le fasce (adatta se hai altre categorie)
    fascia_to_num = {"No picchi": 0, "<50% picchi": 1, ">=50% picchi": 2}

    # costruisci un colorscale "discreto" che associa range a colori
    # struttura: [ [0.0,color_bassa], [0.3333,color_bassa], [0.3334,color_media], ... ]
    colorscale = [
        [0.0, colori["No picchi"]], [0.3333, colori["No picchi"]],
        [0.3334, colori["<50% picchi"]], [0.6666, colori["<50% picchi"]],
        [0.6667, colori[">=50% picchi"]], [1.0, colori[">=50% picchi"]],
    ]

    recapitisti = df_recapitisti["Recapitista"].unique()
    fig_third = go.Figure()

    # crea una traccia (choropleth) per ogni recapitista
    for rec in recapitisti:
        df_temp = df_recapitisti[df_recapitisti["Recapitista"] == rec].copy()
        # mappa le fasce su z numerico
        df_temp["z"] = df_temp["fascia"].map(fascia_to_num)

        # aggiungi la traccia; usiamo customdata per mostrare la fascia testuale nel tooltip
        fig_third.add_trace(go.Choroplethmapbox(
            geojson=geojson,
            locations=df_temp["Regione"],
            z=df_temp["z"],
            featureidkey="properties.reg_name",
            colorscale=colorscale,
            zmin=0, zmax=2,
            marker_opacity=0.6,
            marker_line_width=0,
            name=rec,
            visible=(rec == recapitisti[0]),   # mostra solo il primo inizialmente
            showscale=False,
            customdata=df_temp[["fascia"]].values,
            hovertemplate="<b>%{location}</b><br>Recapitista: " + rec + "<br>Fascia: %{customdata[0]}<extra></extra>"
        ))

    # costruisci i bottoni per il dropdown (uno per recapitista + 'Tutte')
    buttons = []

    # Bottoni singoli
    for i, rec in enumerate(recapitisti):
        visible = [False] * len(recapitisti)
        visible[i] = True
        buttons.append(dict(
            label=rec,
            method="update",
            args=[{"visible": visible}, {"title": f"Assegnazioni - {rec}"}]
        ))

    fig_third.update_layout(
        mapbox_style="carto-positron",
        mapbox_zoom=4.5,
        mapbox_center={"lat": 41.9, "lon": 12.5},
        height=800,
        updatemenus=[dict(buttons=buttons,pad={"r": 20, "t": 20}, direction="down", x=1.12, y=1.12)],
        margin={"r":100,"t":100,"l":100,"b":100},
        title="Assegnazioni per Recapitista (fasce)"
    )

    # istruzione per passare il grafico alla pagina html
    fig_third_for_visualizzation = json.dumps(fig_third, cls=putils.PlotlyJSONEncoder)

    context = {
        'fig_first_for_visualizzation_A': fig_first_for_visualizzation,
        'fig_first_for_visualizzation_B': fig_first_for_visualizzation,
        'fig_second_for_visualizzation_A': fig_second_for_visualizzation,
        'fig_second_for_visualizzation_B': fig_second_for_visualizzation,
        'fig_third_for_visualizzation_A': fig_third_for_visualizzation,
        'fig_third_for_visualizzation_B': fig_third_for_visualizzation
    }
    return render(request, "simulazioni/confronto_risultati.html", context)

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
    lista_mesi_univoci = view_output_capacity_setting.objects.annotate(mese=TruncMonth('ACTIVATION_DATE_FROM')).values_list('mese', flat=True).distinct().order_by('mese')
    lista_mesi_univoci = [(d.strftime("%Y-%m"),d.strftime("%B %Y").capitalize()) for d in lista_mesi_univoci]
    context = {
        'lista_mesi_univoci': lista_mesi_univoci
    }
    # NUOVA SIMULAZIONE
    if id_simulazione == 'new':
        pass
    # MODIFICA SIMULAZIONE
    else:
        simulazione_da_modificare = table_simulazione.objects.get(ID = id_simulazione)
        context['simulazione_da_modificare'] = simulazione_da_modificare
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


def carica_dati_db(request):
    ####### PAGINA PROVVISORIA DI AGGIUNTA DATI #######
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
    ####### PAGINA PROVVISORIA DI AGGIUNTA DATI #######
    return redirect("status")

def rimuovi_dati_db(request):
    ####### PAGINA PROVVISORIA DI RIMOZIONE DATI #######
    conn = psycopg2.connect(database = DATABASES['default']['NAME'],
                            user = DATABASES['default']['USER'],
                            password = DATABASES['default']['PASSWORD'],
                            host = DATABASES['default']['HOST'],
                            port = DATABASES['default']['PORT'])
    cur = conn.cursor()
    cur.execute(f'DROP SCHEMA public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO {DATABASES['default']['USER']}; GRANT ALL ON SCHEMA public TO public;')
    conn.commit()
    conn.close()
    ####### PAGINA PROVVISORIA DI RIMOZIONE DATI #######
    return redirect("status")

# AJAX
def ajax_get_capacita_from_mese_and_tipo(request):
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




# ERROR PAGES
def handle_error_400(request, exception):
    return render(request, 'error_pages/error_400.html')
def handle_error_403(request, exception):
    return render(request, 'error_pages/error_403.html')
def handle_error_404(request, exception):
    return render(request, 'error_pages/error_404.html')
def handle_error_500(request, *args, **argv):
    return render(request, "error_pages/error_500.html", status=500)