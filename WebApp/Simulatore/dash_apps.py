import dash
from dash import dcc, html, Input, Output, dash_table
from django_plotly_dash import DjangoDash
import plotly.graph_objs as go
import plotly.express as px
import numpy as np
import pandas as pd
import os
from PagoPA.settings import *
import json


app_risultati = DjangoDash('dash_risultati')

enti = [
"Regione Lombardia", "INPS", "AMA SPA", "Comune di Roma", "Regione Lazio",
"Poste Italiane", "Agenzia Entrate", "Comune di Milano", "Regione Toscana", "Comune di Napoli"
]

#Simulazione dati per 4 settimane
np.random.seed(42)
dati_enti = []
for ente in enti:
    base = np.random.randint(2000, 8000)  # Valore base per ente
    for settimana in range(1, 5):
        # Fluttuazione con rumore
        valore = base + np.random.randint(-2000, 3000)
        valore = max(500, valore)  # Minimo 500
        dati_enti.append([ente, f"Sett. {settimana}", valore])

# # Creazione DataFrame
df_enti = pd.DataFrame(dati_enti, columns=["Ente", "Settimana", "Postalizzazioni"])

# ---------- dati di esempio (sostituisci con i tuoi)
regioni_province = {
    "Lombardia": ["Milano", "Bergamo", "Brescia"],
    "Lazio": ["Roma", "Latina", "Viterbo"],
    "Campania": ["Napoli", "Salerno", "Caserta"],
    "Sicilia": ["Palermo", "Catania", "Messina"]
}
recapitisti = ["Poste", "Sailpost", "Fulmine", "Express"]

rows = []
for regione, province in regioni_province.items():
    for provincia in province:
        recap = np.random.choice(recapitisti)
        base = np.random.randint(5000, 12000)
        decremento = np.random.randint(800, 2000)
        for settimana in range(1, 5):
            valore = max(500, base - decremento * (settimana - 1))
            rows.append({
                "Regione": regione,
                "Provincia": provincia,
                "Recapitista": recap,
                "Settimana": f"Sett. {settimana}",
                "Postalizzazioni": valore
            })

df_regioni_recap = pd.DataFrame(rows)


np.random.seed(258)
# Creazione dataset
dati_picchi = []
for recapitista in recapitisti:
    test = np.random.choice([0, 10])
    for regione, province in regioni_province.items():
        test = np.random.choice([0, 10])
        if test >= 2:
            for provincia in province:
                valore = np.random.choice([0, 1])
                dati_picchi.append([recapitista, regione, provincia, valore])

df_picchi = pd.DataFrame(dati_picchi, columns=["Recapitista", "Regione", "Provincia", "Assegnazione"])
df_picchi = df_picchi.groupby(['Recapitista','Regione']).agg(
    total_picco=('Assegnazione', 'sum'),
    prov_count=('Provincia', 'count')
)
df_picchi['prop']= df_picchi['total_picco']/df_picchi['prov_count']
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

df_picchi["fascia"] = df_picchi["prop"].apply(classifica_prop)

# Assegno 3 colori fissi
colori = {
    "No picchi": "green",
    "<50% picchi": "orange",
    ">=50% picchi": "red"
}

df_picchi = df_picchi.reset_index()

## mappa testo->numero per le fasce (adatta se hai altre categorie)
fascia_to_num = {"No picchi": 0, "<50% picchi": 1, ">=50% picchi": 2}

# costruisci un colorscale "discreto" che associa range a colori
# struttura: [ [0.0,color_bassa], [0.3333,color_bassa], [0.3334,color_media], ... ]
colorscale = [
    [0.0, colori["No picchi"]], [0.3333, colori["No picchi"]],
    [0.3334, colori["<50% picchi"]], [0.6666, colori["<50% picchi"]],
    [0.6667, colori[">=50% picchi"]], [1.0, colori[">=50% picchi"]],
]

with open(os.path.join(BASE_DIR, 'static/data/limits_IT_regions.json'), encoding = "utf-8") as f:
        geojson = json.load(f)

# layout
app_risultati.layout = html.Div([
    dcc.Location(id="url", refresh=False), # serve a catturare l'url
    html.H2(id="titolo-simulazione",
            style={"text-align":"center"}),
    html.H3(
        "Simulazione Pianificazione Postalizzazioni per Ente",
        style={"text-align":"center"}),


    html.Div([

        html.Div([

            html.Label("Seleziona Ente:"),

            dcc.Dropdown(
                id="ente-filter",
                options=[],
                value=[],
                multi=True
            ),
        ], style={"width": "100%", "display": "inline-block"}),
    ], style={"margin-bottom": "10px"}),

    html.Div([

        dcc.Graph(id="line-plot")

    ]),

    html.H3(
        "Simulazione Pianificazione Postalizzazioni per Provincia e Recapitista",
        style={"text-align":"center"}),

    html.Div([

        html.Div([

            html.Label("Seleziona Regione:"),

            dcc.Dropdown(

                id="regione-filter",

                options=[{"label": r, "value": r} for r in sorted(df_regioni_recap["Regione"].unique())],
                value=list(sorted(df_regioni_recap["Regione"].unique())),
                multi=True,
                placeholder="Seleziona una o più regioni..."
            ),
        ], style={"width": "48%", "display": "inline-block"}),

        html.Div([

            html.Label("Seleziona Recapitista:"),

            dcc.Dropdown(

                id="recap-filter",

                options=[{"label": r, "value": r} for r in sorted(recapitisti)] + [{'label': 'Select all', 'value': 'all_values'}],
                value=list(sorted(recapitisti)),
                multi=True,
                placeholder="Seleziona un recapitista..."
            )
        ], style={"width": "48%", "display": "inline-block", "float": "right"})
    ], style={"margin-bottom": "10px"}),

    html.Div([

        dcc.Graph(id="area-plot")

    ]),

    html.H3(
        "Mappa dei picchi per Recapitista",
        style={"text-align":"center"}),


    html.Div([

        html.Div([

            html.Label("Seleziona Recapitista:"),

            dcc.Dropdown(

                id="recap-only-filter",

                options=[{"label": r, "value": r} for r in sorted(recapitisti)],
                value=recapitisti[0],
                placeholder="Seleziona Recapitista:"
            ),
        ], style={"width": "100%", "display": "inline-block"}),
    ], style={"margin-bottom": "10px"}),

    html.Div([

        dcc.Graph(id="map-plot")

    ]),

    html.Div([

        dash_table.DataTable(
            id='datatable-region',
            data=df_picchi.to_dict('records'),
            columns=[
                {'name': i, 'id': i} for i in df_picchi.columns
            ],
            #style_as_list_view=True,
            style_cell={
                'padding': '5px',
                'textAlign': 'center',
            },
            # style_header={
            #     'backgroundColor': "#585858",
            #     'color': 'white',
            #     'fontWeight': 'bold',

            # },
            #sort_action="native",
            style_data_conditional=[{
            'if': {
                'state': 'active'  # 'active' | 'selected'
                },
            'backgroundColor': 'rgba(0, 116, 217, 0.3)',
            'border': '1px solid rgb(0, 116, 217)'
            }],
            #sort_mode='multi',
            selected_rows=[],
            page_action='native',
            page_current= 0,
            page_size= 20,
        )

    ])
])

@app_risultati.callback(
    Output("titolo-simulazione", "children"),
    Input("url", "pathname")
)
def aggiorna_da_url(pathname):
    from .models import table_simulazione
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    tab_simulazione = table_simulazione.objects.filter(ID = id_simulazione).values()
    df_tab_simulazione = pd.DataFrame(tab_simulazione)
    titolo = "Risultati Simulazione: "+str(df_tab_simulazione['NOME'].values[0])
    return titolo


@app_risultati.callback(
    Output("line-plot", "figure"),
    Input("ente-filter", "value"),
    Input("url", "pathname")
)
def update_chart_ente(ente_sel, pathname):
    from .models import view_output_grafico_ente
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    filtered_enti_2 = view_output_grafico_ente.objects.filter(SIMULAZIONE_ID = id_simulazione).values()
    df_filtered_enti_2 = pd.DataFrame(filtered_enti_2)
    
    # se non selezionato nulla → grafico vuoto
    if not ente_sel:
        return px.line(title="Nessuna selezione effettuata")

    df_filtered_enti_2 = df_filtered_enti_2[df_filtered_enti_2["SENDER_PA_ID"].isin(ente_sel)]

    fig_ente = px.line(
        df_filtered_enti_2,
        x="SETTIMANA_DELIVERY",
        y="COUNT_REQUEST",
        color='SENDER_PA_ID',
        markers=True
    )
    fig_ente.update_layout(legend=dict(x=1.02, y=1, bgcolor="rgba(0,0,0,0)"))

    return fig_ente


@app_risultati.callback(
    Output("ente-filter", "options"),
    Output("ente-filter", "value"),
    Input("url", "pathname")
)
def populate_dropdown(pathname):
    from .models import view_output_grafico_ente
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    lista_enti = view_output_grafico_ente.objects.filter(SIMULAZIONE_ID = id_simulazione).values_list("SENDER_PA_ID", flat=True)
    options = [{"label": r, "value": r} for r in sorted(lista_enti)]
    value = list(sorted(lista_enti))
    return options, value


@app_risultati.callback(
    Output("area-plot", "figure"),
    Input("regione-filter", "value"),
    Input("recap-filter", "value")
)
def update_chart_regioni_recap(regioni_sel, recap_sel):
    # se non selezionato nulla → grafico vuoto
    if not regioni_sel or not recap_sel:
        return px.area(title="Nessuna selezione effettuata")

    filtered_regioni_recap = df_regioni_recap[df_regioni_recap["Regione"].isin(regioni_sel) & df_regioni_recap["Recapitista"].isin(recap_sel)]
    filtered_regioni_recap["Provincia - Recapitista"] = filtered_regioni_recap["Provincia"] + " - " + filtered_regioni_recap["Recapitista"]

    fig_reg_recap = px.area(
        filtered_regioni_recap,
        x="Settimana",
        y="Postalizzazioni",
        color="Provincia - Recapitista",
        line_group="Provincia - Recapitista",
        markers=True
    )
    fig_reg_recap.update_layout(legend=dict(x=1.02, y=1, bgcolor="rgba(0,0,0,0)"))
    return fig_reg_recap
 
@app_risultati.callback(
    Output("map-plot", "figure"),
    Input("recap-only-filter", "value")
)
def update_map_recap(recap_only_sel):
    # se non selezionato nulla → grafico vuoto
    
    filtered_df_picchi = df_picchi[df_picchi["Recapitista"] == recap_only_sel]
    filtered_df_picchi["z"] = filtered_df_picchi["fascia"].map(fascia_to_num)

    fig_picchi = go.Figure()
    fig_picchi = fig_picchi.add_trace(
        go.Choroplethmapbox(
            geojson=geojson,
            locations=filtered_df_picchi["Regione"],
            z=filtered_df_picchi["z"],
            featureidkey="properties.reg_name",
            colorscale=colorscale,
            zmin=0, zmax=2,
            marker_opacity=0.6,
            marker_line_width=0,
            name=recap_only_sel,
            #visible=recap_sel,   # mostra solo il primo inizialmente
            showscale=False,
            customdata=filtered_df_picchi[["fascia"]].values,
            hovertemplate="<b>%{location}</b><br>Recapitista: " + recap_only_sel + "<br>Fascia: %{customdata[0]}<extra></extra>"
        )
    )
    fig_picchi.update_layout(
        mapbox_style="carto-positron",
        mapbox_zoom=4.5,
        mapbox_center={"lat": 41.9, "lon": 12.5},
        height=800,
        #updatemenus=[dict(buttons=buttons,pad={"r": 20, "t": 20}, direction="down", x=1.12, y=1.12)],
        margin={"r":100,"t":100,"l":100,"b":100},
        #title="Assegnazioni per Recapitista (fasce)"
    )
    return fig_picchi

@app_risultati.callback(
    Output("datatable-region", "data"),
    Input("recap-only-filter", "value")
)
def update_table_recap(recap_only_sel):

    filtered_df_picchi = df_picchi[df_picchi["Recapitista"] == recap_only_sel]
    filtered_df_picchi["z"] = filtered_df_picchi["fascia"].map(fascia_to_num)

    return filtered_df_picchi.to_dict("records")



app_confronto = DjangoDash('dash_confronto_risultati')


# layout
app_confronto.layout = html.Div([
    html.H3(
        "Simulazione Pianificazione Postalizzazioni per Ente",
        style={"text-align":"center"}),

    html.Div([
        html.Div([
            html.H4("Simulazione 1",style={"text-align":"center"})
        ], className="col-sm"),
        html.Div([
            html.H4("Simulazione 2",style={"text-align":"center"})
        ], className="col-sm")
    ], className='row text-center'),

    html.Div([
        html.Div([
            html.Div([

                html.Div([

                    html.Label("Seleziona Ente:"),

                    dcc.Dropdown(

                        id="ente-filter-1",

                        options=[{"label": r, "value": r} for r in sorted(enti)],
                        value=list(sorted(enti)),
                        multi=True,
                        placeholder="Seleziona Ente:"
                    ),
                ], style={"width": "100%", "display": "inline-block"}),
            ], style={"margin-bottom": "5px"}),

            html.Div([

                dcc.Graph(id="line-plot-1")

            ]),
        ], className="col-sm"),

        html.Div([

            html.Div([

                html.Div([

                    html.Label("Seleziona Ente:"),

                    dcc.Dropdown(

                        id="ente-filter-2",

                        options=[{"label": r, "value": r} for r in sorted(enti)],
                        value=list(sorted(enti)),
                        multi=True,
                        placeholder="Seleziona Ente:"
                    ),
                ], style={"width": "100%", "display": "inline-block"}),
            ], style={"margin-bottom": "5px"}),

            html.Div([

                dcc.Graph(id="line-plot-2")

            ]),  
        ], className="col-sm"),
    ], className='row text-center'),


    html.H3(
        "Simulazione Pianificazione Postalizzazioni per Provincia e Recapitista",
        style={"text-align":"center"}),


    html.Div([
        html.Div([
            html.H4("Simulazione 1",style={"text-align":"center"})
        ], className="col-sm"),
        html.Div([
            html.H4("Simulazione 2",style={"text-align":"center"})
        ], className="col-sm")
    ], className='row text-center'),

    html.Div([
        html.Div([
            html.Div([

                html.Div([

                    html.Label("Seleziona Regione:"),

                    dcc.Dropdown(

                        id="regione-filter-1",

                        options=[{"label": r, "value": r} for r in sorted(df_regioni_recap["Regione"].unique())],
                        value=list(sorted(df_regioni_recap["Regione"].unique())),
                        multi=True,
                        placeholder="Seleziona una o più regioni..."
                    ),
                ], style={"width": "48%", "display": "inline-block"}),

                html.Div([

                    html.Label("Seleziona Recapitista:"),

                    dcc.Dropdown(

                        id="recap-filter-1",

                        options=[{"label": r, "value": r} for r in sorted(recapitisti)],
                        value=list(sorted(recapitisti)),
                        multi=True,
                        placeholder="Seleziona un recapitista..."
                    )
                ], style={"width": "48%", "display": "inline-block", "float": "right"})
            ], style={"margin-bottom": "5px"}),

            html.Div([

                dcc.Graph(id="area-plot-1")

            ]),
        ], className="col-sm"),

        html.Div([
            html.Div([

                html.Div([

                    html.Label("Seleziona Regione:"),

                    dcc.Dropdown(

                        id="regione-filter-2",

                        options=[{"label": r, "value": r} for r in sorted(df_regioni_recap["Regione"].unique())],
                        value=list(sorted(df_regioni_recap["Regione"].unique())),
                        multi=True,
                        placeholder="Seleziona una o più regioni..."
                    ),
                ], style={"width": "48%", "display": "inline-block"}),

                html.Div([

                    html.Label("Seleziona Recapitista:"),

                    dcc.Dropdown(

                        id="recap-filter-2",

                        options=[{"label": r, "value": r} for r in sorted(recapitisti)],
                        value=list(sorted(recapitisti)),
                        multi=True,
                        placeholder="Seleziona un recapitista..."
                    )
                ], style={"width": "48%", "display": "inline-block", "float": "right"})
            ], style={"margin-bottom": "5px"}),

            html.Div([

                dcc.Graph(id="area-plot-2")

            ]),
        ], className="col-sm"),
    ], className='row text-center'),

    html.H3(
        "Mappa dei picchi per Recapitista",
        style={"text-align":"center"}),

    html.Div([
        html.Div([
            html.H4("Simulazione 1",style={"text-align":"center"})
        ], className="col-sm"),
        html.Div([
            html.H4("Simulazione 2",style={"text-align":"center"})
        ], className="col-sm")
    ], className='row text-center'),

    html.Div([
        html.Div([
            html.Div([

                html.Div([

                    html.Label("Seleziona Recapitista:"),

                    dcc.Dropdown(

                        id="recap-only-filter-1",

                        options=[{"label": r, "value": r} for r in sorted(recapitisti)],
                        value=recapitisti[0],
                        placeholder="Seleziona Recapitista:"
                    ),
                ], style={"width": "100%", "display": "inline-block"}),
            ], style={"margin-bottom": "5px"}),

            html.Div([

                dcc.Graph(id="map-plot-1")

            ]),

            html.Div([

                dash_table.DataTable(
                    id='datatable-region-1',
                    data=df_picchi.to_dict('records'),
                    columns=[
                        {'name': i, 'id': i} for i in df_picchi.columns
                    ],
                    #style_as_list_view=True,
                    style_cell={
                        'padding': '5px',
                        'textAlign': 'center',
                        'minWidth': '60px', 'width': '60px', 'maxWidth': '60px',
                    },
                    # style_header={
                    #     'backgroundColor': "#585858",
                    #     'color': 'white',
                    #     'fontWeight': 'bold',

                    # },
                    #sort_action="native",
                    #style_table={'width': '50%'},
                    style_data_conditional=[{
                    'if': {
                        'state': 'active'  # 'active' | 'selected'
                        },
                    'backgroundColor': 'rgba(0, 116, 217, 0.3)',
                    'border': '1px solid rgb(0, 116, 217)'
                    }],
                    #sort_mode='multi',
                    selected_rows=[],
                    page_action='native',
                    page_current= 0,
                    page_size= 20,
                )

            ]),

        ], className="col-sm"),

        html.Div([
            html.Div([

                html.Div([

                    html.Label("Seleziona Recapitista:"),

                    dcc.Dropdown(

                        id="recap-only-filter-2",

                        options=[{"label": r, "value": r} for r in sorted(recapitisti)],
                        value=recapitisti[0],
                        placeholder="Seleziona Recapitista:"
                    ),
                ], style={"width": "100%", "display": "inline-block"}),
            ], style={"margin-bottom": "5px"}),

            html.Div([

                dcc.Graph(id="map-plot-2")
            ]),

            html.Div([

                dash_table.DataTable(
                    id='datatable-region-2',
                    data=df_picchi.to_dict('records'),
                    columns=[
                        {'name': i, 'id': i} for i in df_picchi.columns
                    ],
                    #style_as_list_view=True,
                    style_cell={
                        'padding': '5px',
                        'textAlign': 'center',
                        'minWidth': '60px', 'width': '60px', 'maxWidth': '60px',
                    },
                    # style_header={
                    #     'backgroundColor': "#585858",
                    #     'color': 'white',
                    #     'fontWeight': 'bold',

                    # },
                    #sort_action="native",
                    style_data_conditional=[{
                    'if': {
                        'state': 'active'  # 'active' | 'selected'
                        },
                    'backgroundColor': 'rgba(0, 116, 217, 0.3)',
                    'border': '1px solid rgb(0, 116, 217)'
                    }],
                    #sort_mode='multi',
                    selected_rows=[],
                    page_action='native',
                    page_current= 0,
                    page_size= 20,
                )
            ])
        ], className="col-sm")
    ], className='row text-center'),
])

@app_confronto.callback(
    Output("line-plot-1", "figure"),
    Input("ente-filter-1", "value")
)
def update_chart_ente_1(ente_sel):
    # se non selezionato nulla → grafico vuoto
    if not ente_sel:
        return px.line(title="Nessuna selezione effettuata")

    filtered_enti = df_enti[df_enti["Ente"].isin(ente_sel)]

    fig_ente = px.line(
        filtered_enti,
        x="Settimana",
        y="Postalizzazioni",
        color='Ente',
        markers=True
    )
    fig_ente.update_layout(legend=dict(x=1.02, y=1, bgcolor="rgba(0,0,0,0)"))
    return fig_ente

@app_confronto.callback(
    Output("line-plot-2", "figure"),
    Input("ente-filter-2", "value")
)
def update_chart_ente_2(ente_sel):
    # se non selezionato nulla → grafico vuoto
    if not ente_sel:
        return px.line(title="Nessuna selezione effettuata")

    filtered_enti = df_enti[df_enti["Ente"].isin(ente_sel)]

    fig_ente = px.line(
        filtered_enti,
        x="Settimana",
        y="Postalizzazioni",
        color='Ente',
        markers=True
    )
    fig_ente.update_layout(legend=dict(x=1.02, y=1, bgcolor="rgba(0,0,0,0)"))
    return fig_ente


@app_confronto.callback(
    Output("area-plot-1", "figure"),
    Input("regione-filter-1", "value"),
    Input("recap-filter-1", "value")
)
def update_chart_regioni_recap_1(regioni_sel, recap_sel):
    # se non selezionato nulla → grafico vuoto
    if not regioni_sel or not recap_sel:
        return px.area(title="Nessuna selezione effettuata")

    filtered_regioni_recap = df_regioni_recap[df_regioni_recap["Regione"].isin(regioni_sel) & df_regioni_recap["Recapitista"].isin(recap_sel)]
    filtered_regioni_recap["Provincia - Recapitista"] = filtered_regioni_recap["Provincia"] + " - " + filtered_regioni_recap["Recapitista"]

    fig_reg_recap = px.area(
        filtered_regioni_recap,
        x="Settimana",
        y="Postalizzazioni",
        color="Provincia - Recapitista",
        line_group="Provincia - Recapitista",
        markers=True
    )
    fig_reg_recap.update_layout(legend=dict(x=1.02, y=1, bgcolor="rgba(0,0,0,0)"))
    return fig_reg_recap


@app_confronto.callback(
    Output("area-plot-2", "figure"),
    Input("regione-filter-2", "value"),
    Input("recap-filter-2", "value")
)
def update_chart_regioni_recap_2(regioni_sel, recap_sel):
    # se non selezionato nulla → grafico vuoto
    if not regioni_sel or not recap_sel:
        return px.area(title="Nessuna selezione effettuata")

    filtered_regioni_recap = df_regioni_recap[df_regioni_recap["Regione"].isin(regioni_sel) & df_regioni_recap["Recapitista"].isin(recap_sel)]
    filtered_regioni_recap["Provincia - Recapitista"] = filtered_regioni_recap["Provincia"] + " - " + filtered_regioni_recap["Recapitista"]

    fig_reg_recap = px.area(
        filtered_regioni_recap,
        x="Settimana",
        y="Postalizzazioni",
        color="Provincia - Recapitista",
        line_group="Provincia - Recapitista",
        markers=True
    )
    fig_reg_recap.update_layout(legend=dict(x=1.02, y=1, bgcolor="rgba(0,0,0,0)"))
    return fig_reg_recap
 
@app_confronto.callback(
    Output("map-plot-1", "figure"),
    Input("recap-only-filter-1", "value")
)
def update_map_recap_1(recap_only_sel):
    # se non selezionato nulla → grafico vuoto
    
    filtered_df_picchi = df_picchi[df_picchi["Recapitista"] == recap_only_sel]
    filtered_df_picchi["z"] = filtered_df_picchi["fascia"].map(fascia_to_num)

    fig_picchi = go.Figure()
    fig_picchi = fig_picchi.add_trace(
        go.Choroplethmapbox(
            geojson=geojson,
            locations=filtered_df_picchi["Regione"],
            z=filtered_df_picchi["z"],
            featureidkey="properties.reg_name",
            colorscale=colorscale,
            zmin=0, zmax=2,
            marker_opacity=0.6,
            marker_line_width=0,
            name=recap_only_sel,
            #visible=recap_sel,   # mostra solo il primo inizialmente
            showscale=False,
            customdata=filtered_df_picchi[["fascia"]].values,
            hovertemplate="<b>%{location}</b><br>Recapitista: " + recap_only_sel + "<br>Fascia: %{customdata[0]}<extra></extra>"
        )
    )
    fig_picchi.update_layout(
        mapbox_style="carto-positron",
        mapbox_zoom=4.5,
        mapbox_center={"lat": 41.9, "lon": 12.5},
        height=800,
        #updatemenus=[dict(buttons=buttons,pad={"r": 20, "t": 20}, direction="down", x=1.12, y=1.12)],
        #margin={"r":100,"t":100,"l":100,"b":100},
        #title="Assegnazioni per Recapitista (fasce)"
    )
    return fig_picchi

@app_confronto.callback(
    Output("datatable-region-1", "data"),
    Input("recap-only-filter-1", "value")
)
def update_table_recap_1(recap_only_sel):

    filtered_df_picchi = df_picchi[df_picchi["Recapitista"] == recap_only_sel]
    filtered_df_picchi["z"] = filtered_df_picchi["fascia"].map(fascia_to_num)

    return filtered_df_picchi.to_dict("records")

@app_confronto.callback(
    Output("map-plot-2", "figure"),
    Input("recap-only-filter-2", "value")
)
def update_map_recap_2(recap_only_sel):
    # se non selezionato nulla → grafico vuoto
    
    filtered_df_picchi = df_picchi[df_picchi["Recapitista"] == recap_only_sel]
    filtered_df_picchi["z"] = filtered_df_picchi["fascia"].map(fascia_to_num)

    fig_picchi = go.Figure()
    fig_picchi = fig_picchi.add_trace(
        go.Choroplethmapbox(
            geojson=geojson,
            locations=filtered_df_picchi["Regione"],
            z=filtered_df_picchi["z"],
            featureidkey="properties.reg_name",
            colorscale=colorscale,
            zmin=0, zmax=2,
            marker_opacity=0.6,
            marker_line_width=0,
            name=recap_only_sel,
            #visible=recap_sel,   # mostra solo il primo inizialmente
            showscale=False,
            customdata=filtered_df_picchi[["fascia"]].values,
            hovertemplate="<b>%{location}</b><br>Recapitista: " + recap_only_sel + "<br>Fascia: %{customdata[0]}<extra></extra>"
        )
    )
    fig_picchi.update_layout(
        mapbox_style="carto-positron",
        mapbox_zoom=4.5,
        mapbox_center={"lat": 41.9, "lon": 12.5},
        height=800,
        #updatemenus=[dict(buttons=buttons,pad={"r": 20, "t": 20}, direction="down", x=1.12, y=1.12)],
        #margin={"r":100,"t":100,"l":100,"b":100},
        #title="Assegnazioni per Recapitista (fasce)"
    )
    return fig_picchi

@app_confronto.callback(
    Output("datatable-region-2", "data"),
    Input("recap-only-filter-2", "value")
)
def update_table_recap_2(recap_only_sel):

    filtered_df_picchi = df_picchi[df_picchi["Recapitista"] == recap_only_sel]
    filtered_df_picchi["z"] = filtered_df_picchi["fascia"].map(fascia_to_num)

    return filtered_df_picchi.to_dict("records")



