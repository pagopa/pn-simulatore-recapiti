from django.shortcuts import render

import plotly.graph_objects as go
import json
import plotly.utils as putils
import plotly.express as px
import requests
import pandas as pd
import numpy as np
import os
from PagoPA.settings import *

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
        mapbox_zoom=5,
        mapbox_center={"lat": 41.9, "lon": 12.5},
        height=1000,
        updatemenus=[dict(buttons=buttons,pad={"r": 20, "t": 20}, direction="down", x=1.12, y=1.12)],
        margin={"r":100,"t":100,"l":100,"b":100},
        title="Assegnazioni per Recapitista (fasce)"
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