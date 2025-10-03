from django.shortcuts import render

import plotly.graph_objects as go
import json
import plotly.utils as putils
import plotly.express as px
import requests
import pandas as pd

def homepage(request):
    lista_simulazioni = [
        {
            'id': '0001',
            'timestamp_esecuzione': '2025/08/10 17:00:00',
            'stato': 'Schedulata',
            'utente': 'Paolo Bianchi',
            'nome': 'Test 4',
            'descrizione': 'Test con leggero aumento delle capacità',
            'errore': None,
            'durata': None,
        },
        {
            'id': '0002',
            'timestamp_esecuzione': '2025/07/23 15:00:00',
            'stato': 'In lavorazione',
            'utente': 'Mario Rossi',
            'nome': 'Test 3',
            'descrizione': 'Test con leggera diminuzione delle capacità',
            'errore': None,
            'durata': None,
        },
        {
            'id': '0003',
            'timestamp_esecuzione': '2025/07/23 10:00:00',
            'stato': 'Lavorata',
            'utente': 'Mario Rossi',
            'nome': 'Test 2',
            'descrizione': 'Test con drastico aumento delle capacità',
            'errore': None,
            'durata': '0:13:50'
        },
        {
            'id': '0004',
            'timestamp_esecuzione': '2025/07/23 10:00:00',
            'stato': 'Lavorata',
            'utente': '',
            'nome': 'Automatizzata',
            'descrizione': 'Pianificazione settimanale automatizzata',
            'errore': None,
            'durata': '0:13:50'
        },
        {
            'id': '0005',
            'timestamp_esecuzione': '2025/07/22 11:30:00',
            'stato': 'Non completata',
            'utente': 'Luca Neri',
            'nome': 'Test 1',
            'descrizione': 'Test con drastica diminuzione delle capacità',
            'errore': '429 - Algoritmo di pianificazione occupato',
            'durata': None,
        }
    ]

    context = {
        'lista_simulazioni': lista_simulazioni
    }
    return render(request, "home.html", context)


def risultati(request, id_simulazione):

    df = px.data.gapminder().query("continent == 'Oceania'")
    fig_first = px.line(df, x='year', y='lifeExp', color='country', markers=True)

    # istruzione per passare il grafico alla pagina html
    fig_first_for_visualizzation = json.dumps(fig_first, cls=putils.PlotlyJSONEncoder)
    
    df2 = px.data.gapminder()
    fig_second = px.area(df2, x="year", y="pop", color="continent", line_group="country")

    # istruzione per passare il grafico alla pagina html
    fig_second_for_visualizzation = json.dumps(fig_second, cls=putils.PlotlyJSONEncoder)

    # Scarico un geojson delle regioni italiane
    url = "https://raw.githubusercontent.com/openpolis/geojson-italy/master/geojson/limits_IT_regions.geojson"
    geojson = requests.get(url).json()

    # Dataset esempio: popolazione per regione
    data = {
        "regione": [
            "Lombardia", "Lazio", "Campania", "Sicilia", "Veneto", "Emilia-Romagna",
            "Piemonte", "Puglia", "Toscana", "Calabria", "Sardegna", "Liguria",
            "Marche", "Abruzzo", "Friuli-Venezia Giulia", "Trentino-Alto Adige/Südtirol",
            "Umbria", "Basilicata", "Molise", "Valle d'Aosta"
        ],
        "popolazione": [
            10060574, 5879082, 5801692, 4999891, 4905854, 4459477,
            4356406, 4029053, 3729641, 1830951, 1554505, 1507859,
            1501075, 1311580, 1211357, 1072276, 888908, 553254,
            300516, 125666
        ]
    }
    df3 = pd.DataFrame(data)

    # Definisco due soglie (puoi cambiarle come preferisci)
    soglia1 = 2_000_000
    soglia2 = 5_000_000

    # Creo una nuova colonna "fascia"
    def classifica_pop(x):
        if x < soglia1:
            return "Bassa"
        elif x < soglia2:
            return "Media"
        else:
            return "Alta"

    df3["fascia"] = df3["popolazione"].apply(classifica_pop)

    # Assegno 3 colori fissi
    colori = {
        "Bassa": "green",
        "Media": "orange",
        "Alta": "red"
    }

    # Mappa coropletica a categorie
    fig_third = px.choropleth_mapbox(
        df3,
        geojson=geojson,
        locations="regione",
        featureidkey="properties.reg_name",
        color="fascia",
        color_discrete_map=colori,
        mapbox_style="carto-positron",
        zoom=4.5, center={"lat": 41.9, "lon": 12.5},
        opacity=0.6
    )

    # istruzione per passare il grafico alla pagina html
    fig_third_for_visualizzation = json.dumps(fig_third, cls=putils.PlotlyJSONEncoder)

    context = {
        'fig_first_for_visualizzation': fig_first_for_visualizzation,
        'fig_second_for_visualizzation': fig_second_for_visualizzation,
        'fig_third_for_visualizzation': fig_third_for_visualizzation
    }
    return render(request, "simulazioni/risultati.html", context)


def confronto_risultati(request, id_simulazione):

    df = px.data.gapminder().query("continent == 'Oceania'")
    fig_first = px.line(df, x='year', y='lifeExp', color='country', markers=True)

    # istruzione per passare il grafico alla pagina html
    fig_first_for_visualizzation = json.dumps(fig_first, cls=putils.PlotlyJSONEncoder)
    
    df2 = px.data.gapminder()
    fig_second = px.area(df2, x="year", y="pop", color="continent", line_group="country")

    # istruzione per passare il grafico alla pagina html
    fig_second_for_visualizzation = json.dumps(fig_second, cls=putils.PlotlyJSONEncoder)

    # Scarico un geojson delle regioni italiane
    url = "https://raw.githubusercontent.com/openpolis/geojson-italy/master/geojson/limits_IT_regions.geojson"
    geojson = requests.get(url).json()

    # Dataset esempio: popolazione per regione
    data = {
        "regione": [
            "Lombardia", "Lazio", "Campania", "Sicilia", "Veneto", "Emilia-Romagna",
            "Piemonte", "Puglia", "Toscana", "Calabria", "Sardegna", "Liguria",
            "Marche", "Abruzzo", "Friuli-Venezia Giulia", "Trentino-Alto Adige/Südtirol",
            "Umbria", "Basilicata", "Molise", "Valle d'Aosta"
        ],
        "popolazione": [
            10060574, 5879082, 5801692, 4999891, 4905854, 4459477,
            4356406, 4029053, 3729641, 1830951, 1554505, 1507859,
            1501075, 1311580, 1211357, 1072276, 888908, 553254,
            300516, 125666
        ]
    }
    df3 = pd.DataFrame(data)

    # Definisco due soglie (puoi cambiarle come preferisci)
    soglia1 = 2_000_000
    soglia2 = 5_000_000

    # Creo una nuova colonna "fascia"
    def classifica_pop(x):
        if x < soglia1:
            return "Bassa"
        elif x < soglia2:
            return "Media"
        else:
            return "Alta"

    df3["fascia"] = df3["popolazione"].apply(classifica_pop)

    # Assegno 3 colori fissi
    colori = {
        "Bassa": "green",
        "Media": "orange",
        "Alta": "red"
    }

    # Mappa coropletica a categorie
    fig_third = px.choropleth_mapbox(
        df3,
        geojson=geojson,
        locations="regione",
        featureidkey="properties.reg_name",
        color="fascia",
        color_discrete_map=colori,
        mapbox_style="carto-positron",
        zoom=4.5, center={"lat": 41.9, "lon": 12.5},
        opacity=0.6
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
    lista_bozze = [
        {
            'timestamp_scheduling': '2025/07/23 15:00:00',
            'nome': 'Test 3',
            'utente': 'Mario Rossi',
            'descrizione': 'Test 5'
        },
        {
            'timestamp_scheduling': '2025/07/23 10:00:00',
            'nome': 'Test 2',
            'utente': 'Mario Rossi',
            'descrizione': 'Test 2'
        },
        {
            'timestamp_scheduling': '2025/07/22 11:30:00',
            'nome': 'Test 1',
            'utente': 'Paolo Bianchi',
            'descrizione': 'Test 1'
        }
    ]

    context = {
        'lista_bozze': lista_bozze
    }
    return render(request, "bozze/bozze.html", context)

def nuova_simulazione(request):
    return render(request, "simulazioni/nuova_simulazione.html")

def login(request):
    return render(request, "login.html")



# ERROR PAGES
def handle_error_400(request, exception):
    return render(request, 'error_pages/error_400.html')
def handle_error_403(request, exception):
    return render(request, 'error_pages/error_403.html')
def handle_error_404(request, exception):
    return render(request, 'error_pages/error_404.html')
def handle_error_500(request, *args, **argv):
    return render(request, "error_pages/error_500.html", status=500)