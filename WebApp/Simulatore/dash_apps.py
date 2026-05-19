import dash
from dash import dcc, html, Input, Output, dash_table
from django_plotly_dash import DjangoDash
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
import os
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from PagoPA.settings import *
import json
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
from django.db.models import F
# import datetime

# for js in dag.AgGrid._js_dist:     
#     js['external_url'] = js['external_url'].replace('35.2.0', '31.2.0')


app_risultati = DjangoDash('dash_risultati', external_stylesheets=[dbc.themes.FLATLY])

# caricamento dati json con i limiti di ogni regione come coordinate
with open(os.path.join(BASE_DIR, 'static/data/limits_IT_regions.json'), encoding = "utf-8") as f:
        geojson = json.load(f)

# layout
app_risultati.layout = html.Div([
    dcc.Location(id="url", refresh=False), # serve a catturare l'url

    # titolo della simulazione
    html.H2(id="titolo-simulazione",
            style={"text-align":"center"}),
    # titolo del grafico
    html.Div([
    
        html.H5(
            "SIMULAZIONE PIANIFICAZIONE POSTALIZZAZIONI PER ENTE",
            style={"text-align":"left"}
            ),

        # Div selezione ente
        html.Div([
            html.Div([
                html.Label("Seleziona Ente:"),
                dcc.Dropdown(
                    id="ente-filter",
                    options=[],
                    value=[],
                    multi=True
                ),
            ], style={"width": "52%", "display": "inline-block"}),

            html.Div([
                html.Label("Seleziona Ordinamento:"),
                dcc.Dropdown(
                    id="sorting-filter",
                    options=['Num. di Postalizzazioni (DESC)','Num. di Postalizzazioni (ASC)', 'Num. di Residui (DESC)', 'Num. di Residui (ASC)'],
                    value='Num. di Postalizzazioni (DESC)',
                    clearable=False,
                    multi=False,
                    searchable=False,
                    placeholder="Seleziona un ordinamento..."
                )
            ], style={"width": "22%", "height":"200%", "display": "inline-block"}
            ),

            html.Div([
                html.Label("Seleziona Num. di Enti:"),
                dcc.Dropdown(
                    id="selecting-filter",
                    options=[
                        {'label': '5', 'value': 5},
                        {'label': '10', 'value': 10},
                        {'label': '20', 'value': 20}
                    ],
                    value=5,
                    multi=False,
                    clearable=False,
                    searchable=False,
                    placeholder="Seleziona un numero..."
                )
            ], style={"width": "14%",  "display": "inline-block"}
            )
        ], style={
            'display': 'flex', 
            'flexDirection': 'row', 
            'width': '100%',
            'gap': '10px',"margin-bottom": "10px"
            }
        ),
        # Div grafico
        html.Div([ dcc.Graph(id="line-plot",
                             config={
                                    "toImageButtonOptions": {
                                        "filename": "grafico_per_ente", # Do not add .png; it is appended automatically
                                        "scale": 2                     # Optional: improves image resolution
                                    },
                                    'modeBarButtonsToRemove': ['select2d', 'lasso2d'],
                                    'displaylogo': False
                             }
        ) ]),
    
        html.Div([
            # Margine superiore
            dag.AgGrid(
                id="datatable-enti-ag",
                rowData=[],  # Equivalente a 'data'
                columnDefs=[
                    # Esempio: {"field": "NomeColonna", "headerName": "Etichetta"}
                ],
                defaultColDef={
                    "headerClass": 'center-aligned-header',
                    "resizable": True,
                    "sortable": True,
                    "filter": True,
                    "flex": 1,         # Distribuisce le colonne uniformemente
                    "minWidth": 100,
                    "cellStyle": {"textAlign": "center"},
                },
                dashGridOptions={
                    "domLayout": 'autoHeight',
                    "pagination": True,
                    "paginationPageSize": 10,
                    "paginationPageSizeSelector": [10, 20, 50, 100],
                    "rowSelection": "single", # Sostituisce selected_rows
                    "theme": {
                            "function": "themeBalham.withParams({ spacing: 4 })"
                        }
                },
                csvExportParams={
                    "fileName": "dati_enti_export.csv",
                },
                className="ag-theme-balham", # Tema moderno (chiaro) o "ag-theme-alpine-dark"
            ),
            dbc.Row([
                    dbc.Col([
                        dbc.Button(
                            [
                                html.I(className="fa-solid fa-download me-2"), # Icona (richiede FontAwesome)
                                "Scarica CSV"
                            ],
                            id="btn-export-enti",
                            color="success",    # Verde (tipico per download/export)
                            outline=True,       # Stile moderno con bordo, si riempie al passaggio del mouse
                            size="sd",          # Dimensione media (sm, md, lg)
                            className="mb-3",   # Margine in basso per staccarlo dalla tabella
                        )
                    ], width="auto")
                ],
                justify="end",
                className="mt-3")
        ]),
    
    ], className="pretty_container"),

    

    html.Div([
        html.H5(
            "SIMULAZIONE PIANIFICAZIONE POSTALIZZAZIONI PER PROVINCIA E PER RECAPITISTA",
            style={"text-align":"left"}),

        html.Div([

            html.Div([

                html.Label("Seleziona Regione:"),

                dcc.Dropdown(

                    id="regione-filter",
                    
                    options=[],
                    value=[],
                    multi=True,
                    placeholder="Seleziona una o più regioni..."
                ),
            ], style={"width": "48%", "display": "inline-block"}),

            html.Div([

                html.Label("Seleziona Recapitista:"),

                dcc.Dropdown(

                    id="recap-filter",

                    options=[],
                    value=[],
                    multi=True,
                    placeholder="Seleziona un recapitista..."
                )
            ], style={"width": "48%", "display": "inline-block", "float": "right"})
        ], style={"margin-bottom": "10px"}),

        
        html.Div([

            dcc.Graph(id="line-reg-plot",
                             config={
                                    "toImageButtonOptions": {
                                        "filename": "grafico_per_regione_recapitista", # Do not add .png; it is appended automatically
                                        "scale": 2                     # Optional: improves image resolution
                                    },
                                    'modeBarButtonsToRemove': ['select2d', 'lasso2d'],
                                    'displaylogo': False
                             })
        ]),

        html.Div([
            # Margine superiore
            dag.AgGrid(
                id="datatable-reg-recap-ag",
                rowData=[],  # Equivalente a 'data'
                columnDefs=[
                    # Esempio: {"field": "NomeColonna", "headerName": "Etichetta"}
                ],
                defaultColDef={
                    "headerClass": 'center-aligned-header',
                    "resizable": True,
                    "sortable": True,
                    "filter": True,
                    "flex": 1,         # Distribuisce le colonne uniformemente
                    "minWidth": 100,
                    "cellStyle": {"textAlign": "center"},
                },
                dashGridOptions={
                    "domLayout": 'autoHeight',
                    "pagination": True,
                    "paginationPageSize": 10,
                    "paginationPageSizeSelector": [10, 20, 50, 100],
                    "rowSelection": "single", # Sostituisce selected_rows
                    "theme": {
                            "function": "themeBalham.withParams({ spacing: 4 })"
                        }
                },
                csvExportParams={
                    "fileName": "dati_recapitisti_export.csv",
                },
                className="ag-theme-balham", # Tema moderno (chiaro) o "ag-theme-alpine-dark"
            ),
            dbc.Row([
                    dbc.Col([
                        dbc.Button(
                            [
                                html.I(className="fa-solid fa-download me-2"), # Icona (richiede FontAwesome)
                                "Scarica CSV"
                            ],
                            id="btn-export-reg-recap",
                            color="success",    # Verde (tipico per download/export)
                            outline=True,       # Stile moderno con bordo, si riempie al passaggio del mouse
                            size="sd",          # Dimensione media (sm, md, lg)
                            className="mb-3",   # Margine in basso per staccarlo dalla tabella
                        )
                    ], width="auto")
                ],
                justify="end",
                className="mt-3")
        ])
    ], className="pretty_container"),

    html.Div([
        html.H5(
            "SIMULAZIONE PIANIFICAZIONE POSTALIZZAZIONI PER RECAPITISTA",
            style={"text-align":"left"}),
        html.Div([

            dcc.Graph(id="area-plot",
                      config={
                            "toImageButtonOptions": {
                                "filename": "grafico_per_recapitista", # Do not add .png; it is appended automatically
                                "scale": 2                     # Optional: improves image resolution
                            },
                            'modeBarButtonsToRemove': ['select2d', 'lasso2d'],
                            'displaylogo': False
                        })

        ]),
    ], className="pretty_container"),

    html.Div([
        html.H5(
            "SIMULAZIONE PIANIFICAZIONE POSTALIZZAZIONI SU BASE GEOGRAFICA",
            style={"text-align":"left"}),
        html.Div([

            html.Div([

                html.Label("Seleziona Recapitista:"),

                dcc.Dropdown(

                    id="recap-only-filter",

                    options=[],
                    value=[],
                    placeholder="Seleziona un recapitista..."
                ),
            ], style={"width": "100%", "display": "inline-block"}),
        ], style={"margin-bottom": "10px"}),

        html.Div([
            html.Div([

                dcc.Graph(id="map-plot",
                            config={
                                    "toImageButtonOptions": {
                                        "filename": "mappa_dei_picchi", # Do not add .png; it is appended automatically
                                        "scale": 2                     # Optional: improves image resolution
                                    },
                                    'modeBarButtonsToRemove': ['select2d', 'lasso2d'],
                                    'displaylogo': False
                             })

            ], style={"width": "60%", "display": "inline-block"}),
        
            html.Div([
                # Margine superiore
                dag.AgGrid(
                    id="datatable-region-ag",
                    rowData=[],  # Equivalente a 'data'
                    columnDefs=[
                        # Esempio: {"field": "NomeColonna", "headerName": "Etichetta"}
                    ],
                    defaultColDef={
                        "headerClass": 'center-aligned-header',
                        "resizable": True,
                        "sortable": True,
                        "filter": True,
                        "flex": 1,         # Distribuisce le colonne uniformemente
                        "minWidth": 100,
                        "cellStyle": {"textAlign": "center"},
                    },
                    rowClassRules = {"bg-danger": "params.data.Picco == 'Presente'"},
                    dashGridOptions={
                        "domLayout": 'autoHeight',
                        "pagination": True,
                        "paginationPageSize": 20,
                        "paginationPageSizeSelector": False,
                        "rowSelection": "single", # Sostituisce selected_rows
                        "theme": {
                            "function": "themeBalham.withParams({ spacing: 4 })"
                        }
                    },
                    csvExportParams={
                        "fileName": "dati_picco_export.csv",
                    },
                    className="ag-theme-balham", # Tema moderno (chiaro) o "ag-theme-alpine-dark"
                ),
                dbc.Row([
                        dbc.Col([
                            dbc.Button(
                                [
                                    html.I(className="fa-solid fa-download me-2"), # Icona (richiede FontAwesome)
                                    "Scarica CSV"
                                ],
                                id="btn-export",
                                color="success",    # Verde (tipico per download/export)
                                outline=True,       # Stile moderno con bordo, si riempie al passaggio del mouse
                                size="sd",          # Dimensione media (sm, md, lg)
                                className="mb-3",   # Margine in basso per staccarlo dalla tabella
                            )
                        ], width="auto")
                    ],
                    justify="end",
                    className="mt-3")
            ], style={"width": "38%", "margin-top": "100px", "display": "inline-block", "float": "right"})
        ])
    ], className="pretty_container"),
])


@app_risultati.callback(
    Output("titolo-simulazione", "children"),
    Input("url", "pathname")
)
def aggiorna_da_url(pathname):
    from .models import table_simulazione
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    nome_simulazione = table_simulazione.objects.filter(ID = id_simulazione).values_list("NOME", flat=True)[0]
    titolo = "Risultati Simulazione: "+str(nome_simulazione)
    return titolo


@app_risultati.callback(
    Output("ente-filter", "options"),
    Output("ente-filter", "value"),
    Input("url", "pathname"),
    Input("sorting-filter","value"),
    Input("selecting-filter","value")
)
def populate_dropdown_ente(pathname, value, value_sel):
    from .models import view_tabella_sintesi_ente
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    sorting_value = value
    if ("Postalizzazioni" in sorting_value):
        if ("DESC" in sorting_value):
            tab_sintesi_ente = view_tabella_sintesi_ente.objects.filter(SIMULAZIONE_ID = id_simulazione).order_by("-SUM_COUNT_REQUEST").values()
        else:
            tab_sintesi_ente = view_tabella_sintesi_ente.objects.filter(SIMULAZIONE_ID = id_simulazione).order_by("SUM_COUNT_REQUEST").values()
    else:
        if ("DESC" in sorting_value):
            tab_sintesi_ente = view_tabella_sintesi_ente.objects.filter(SIMULAZIONE_ID = id_simulazione).order_by(F('SUM_COUNT_RESIDUI').desc(nulls_last=True)).values()
        else:
            tab_sintesi_ente = view_tabella_sintesi_ente.objects.filter(SIMULAZIONE_ID = id_simulazione).order_by(F('SUM_COUNT_RESIDUI').asc(nulls_last=True)).values()
    df_enti = pd.DataFrame(list(tab_sintesi_ente))
    options = [{"label": r, "value": r} for r in list(df_enti["SENDER_PA_ID"])]
    value = list(df_enti["SENDER_PA_ID"])[:value_sel]

    return options, value


@app_risultati.callback(
    Output("line-plot", "figure"),
    Input("ente-filter", "value"),
    Input("url", "pathname")
)
def update_chart_ente(ente_sel, pathname):
    from .models import table_output_grafico_ente
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    filtered_enti = table_output_grafico_ente.objects.filter(SIMULAZIONE_ID = id_simulazione, SENDER_PA_ID__in = ente_sel).order_by('SETTIMANA_DELIVERY').values()
    df_filtered_enti = pd.DataFrame(filtered_enti)
    
    # se non selezionato nulla → grafico vuoto
    if not ente_sel:
        return px.line(title="Nessuna selezione effettuata")
    
    fig_ente = px.line(
        df_filtered_enti,
        x="SETTIMANA_DELIVERY",
        y="COUNT_REQUEST",
        color='SENDER_PA_ID',
        markers=True,
        
    )

    fig_ente.update_traces(
        hovertemplate="<br>".join([
            "<b>ID Ente: %{fullData.name}</b>", # Prende il nome dalla traccia (colore)
            "Settimana: %{x|%d-%m-%Y}",
            "Postalizzazioni: <b>%{y:,}</b>",
            "<extra></extra>" # Rimuove il box con il nome della serie a destra
        ])
    )

    fig_ente.update_layout(
        template="none",
        legend=dict(
            title=dict(
                text="ID Ente"
            ),
            x=1.02, 
            y=1, 
            bgcolor="rgba(0,0,0,0)"
        ),
        xaxis=dict(
            title=dict(
                text="Settima di Delivery"
            )
        ),
        yaxis=dict(
            title=dict(
                text="Numero di Postalizzazioni"
            )
        )
    )
    return fig_ente


@app_risultati.callback(
    Output("datatable-enti-ag", "rowData"),
    Output("datatable-enti-ag", "columnDefs"),
    Input("url", "pathname"),
    Input("sorting-filter","value")
)
def update_table_ente_ag(pathname, value):
    from .models import view_tabella_sintesi_ente
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    sorting_value = value
    #lista_enti_2 = table_output_grafico_ente.objects.filter(SIMULAZIONE_ID = id_simulazione).order_by('-COUNT_REQUEST').values_list("SENDER_PA_ID", flat=True)
    if ("Postalizzazioni" in sorting_value):
        if ("DESC" in sorting_value):
            tab_sintesi_ente = view_tabella_sintesi_ente.objects.filter(SIMULAZIONE_ID = id_simulazione).order_by("-SUM_COUNT_REQUEST").values()
        else:
            tab_sintesi_ente = view_tabella_sintesi_ente.objects.filter(SIMULAZIONE_ID = id_simulazione).order_by("SUM_COUNT_REQUEST").values()
    else:
        if ("DESC" in sorting_value):
            tab_sintesi_ente = view_tabella_sintesi_ente.objects.filter(SIMULAZIONE_ID = id_simulazione).order_by(F('SUM_COUNT_RESIDUI').desc(nulls_last=True)).values()
        else:
            tab_sintesi_ente = view_tabella_sintesi_ente.objects.filter(SIMULAZIONE_ID = id_simulazione).order_by(F('SUM_COUNT_RESIDUI').asc(nulls_last=True)).values()
    df_tab_sintesi_ente = pd.DataFrame(tab_sintesi_ente)
    df_tab_sintesi_ente.drop(columns=["id","SIMULAZIONE_ID_id"], axis=1, inplace=True)
    df_tab_sintesi_ente = df_tab_sintesi_ente.rename(columns={"SENDER_PA_ID": "ID Ente", "SUM_COUNT_REQUEST": "Volumi Totali", "AVG_COUNT_REQUEST": "Volumi Medi Settimanali", "SUM_COUNT_RESIDUI": "Volumi Residui"})
    rowData = df_tab_sintesi_ente.to_dict("records")
    columnDefs = []
    for i in df_tab_sintesi_ente.columns:
        column_spec = {"field": i}
        if i not in ("ID Ente","Volumi Medi Settimanali"):
            column_spec["valueFormatter"] = {"function": "d3.format(',')(params.value)"}
        # Esempio: se la colonna è numerica, formatta con 1 decimale
        if i == "Volumi Medi Settimanali":
            column_spec["valueFormatter"] = {"function": "d3.format(',.1f')(params.value)"}
        columnDefs.append(column_spec)
    return rowData, columnDefs

@app_risultati.callback(
    Output("datatable-enti-ag", "exportDataAsCsv"),
    Input("btn-export-enti", "n_clicks"),
    prevent_initial_call=True,
)
def export_data_enti(n_clicks):
    if n_clicks > 0:
        return True
    return False

@app_risultati.callback(
    Output("regione-filter", "options"),
    Output("regione-filter", "value"),
    Input("url", "pathname")
)
def populate_dropdown_regione(pathname):
    from .models import table_output_grafico_reg_recap
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    lista_regioni = table_output_grafico_reg_recap.objects.filter(SIMULAZIONE_ID = id_simulazione).values_list("REGIONE", flat=True)
    options = [{"label": r, "value": r} for r in sorted(list(set(lista_regioni)))]
    value = sorted(list(set(lista_regioni)))
    return options, value

@app_risultati.callback(
    Output("recap-filter", "options"),
    Output("recap-filter", "value"),
    Input("url", "pathname")
)
def populate_dropdown_recap(pathname):
    from .models import table_output_grafico_reg_recap
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    lista_recap = table_output_grafico_reg_recap.objects.filter(SIMULAZIONE_ID = id_simulazione).values_list("UNIFIED_DELIVERY_DRIVER", flat=True)
    options = [{"label": r, "value": r} for r in sorted(list(set(lista_recap)))]
    value = sorted(list(set(lista_recap)))
    return options, value


@app_risultati.callback(
    Output("line-reg-plot", "figure"),
    Input("regione-filter", "value"),
    Input("recap-filter", "value"),
    Input("url", "pathname")
)
def update_linechart_regioni_recap(regioni_sel, recap_sel, pathname):
    from .models import table_output_grafico_reg_recap
    from .models import view_tabella_sintesi_reg_recap
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    if isinstance(regioni_sel, str):
        regioni_sel = [regioni_sel]
    regioni_recap = table_output_grafico_reg_recap.objects.filter(SIMULAZIONE_ID = id_simulazione, REGIONE__in = regioni_sel, UNIFIED_DELIVERY_DRIVER__in = recap_sel).values()
    # se non selezionato nulla → grafico vuoto
    if not regioni_sel or not recap_sel:
        return px.area(title="Nessuna selezione effettuata", template="none")
    prov_recap_5_sett = view_tabella_sintesi_reg_recap.objects.filter(SIMULAZIONE_ID = id_simulazione).order_by(F('SUM_COUNT_REQUEST_5_SETT').desc(nulls_last=True)).values("PROVINCIA_RECAPITISTA")
    df_prov_recap_5_sett = pd.DataFrame(prov_recap_5_sett)
    lista_prov_recap_5_sett = df_prov_recap_5_sett['PROVINCIA_RECAPITISTA'].tolist()
    df_regioni_recap = pd.DataFrame(regioni_recap)
    # df_regioni_recap["Provincia_Recapitista"] = df_regioni_recap["PROVINCE"] + " - " + df_regioni_recap["UNIFIED_DELIVERY_DRIVER"]
    fig_line_reg_recap = px.line(
        df_regioni_recap,
        x="SETTIMANA_DELIVERY",
        y="COUNT_REQUEST",
        color="PROVINCIA_RECAPITISTA",
        line_group="PROVINCIA_RECAPITISTA",
        category_orders={
            "PROVINCIA_RECAPITISTA": lista_prov_recap_5_sett
        },
        markers=True
    )
    fig_line_reg_recap.update_traces(
        hovertemplate="<br>".join([
            "<b>Provincia - Recapitista: %{fullData.name}</b>", # Prende il nome dalla traccia (colore)
            "Settimana: %{x|%d-%m-%Y}",
            "Postalizzazioni: <b>%{y:,}</b>",
            "<extra></extra>" # Rimuove il box con il nome della serie a destra
        ])
    )

    fig_line_reg_recap.update_layout(
        template="none",
        legend=dict(
            title=dict(
                text="Provincia - Recapitista"
            ),
            x=1.02, 
            y=1, 
            bgcolor="rgba(0,0,0,0)"
        ),
        xaxis=dict(
            title=dict(
                text="Settima di Delivery"
            )
        ),
        yaxis=dict(
            title=dict(
                text="Numero di Postalizzazioni"
            )
        )
    )

    # calcolo del primo lunedì del mese successivo al mese selezionato dall'utente per la simulazione
    anno = df_regioni_recap['SETTIMANA_DELIVERY'].min().year
    mese = df_regioni_recap['SETTIMANA_DELIVERY'].min().month
    primo_lunedi_mese_successivo = date(anno, mese, 1) + relativedelta(months=+1)
    offset = (0 - primo_lunedi_mese_successivo.weekday()) % 7 # RICORDA: con weekday(), 0=lunedì, 6=domenica
    primo_lunedi_mese_successivo = pd.Timestamp(primo_lunedi_mese_successivo + timedelta(days=offset))
    primo_lunedi_mese_successivo = primo_lunedi_mese_successivo.to_datetime64()

    # Cicliamo tra le tracce del grafico per modificarle
    for trace in fig_line_reg_recap.data:
        # Recuperiamo i dati di questa specifica linea
        # Filtriamo i dati per considerare solo quelli dopo la data X
        mask = df_regioni_recap['SETTIMANA_DELIVERY'] > primo_lunedi_mese_successivo
        dati_post_rottura = df_regioni_recap[(df_regioni_recap['PROVINCIA_RECAPITISTA'] == trace.name) & mask]
        
        # Controlliamo se in questo set di dati qualcuno supera la soglia
        if any(dati_post_rottura['SETTIMANA_DELIVERY'] > primo_lunedi_mese_successivo):
            # Cambiamo il nome della traccia aggiungendo il grassetto HTML
            trace.name = f"<b>{trace.name}</b>"
            # Opzionale: possiamo anche aumentare lo spessore della linea per farla risaltare
            trace.line.width = 3
        else:
            # Opzionale: rendiamo le altre linee leggermente più trasparenti
            trace.opacity = 0.5

    data_linea = (primo_lunedi_mese_successivo + timedelta(days=3.5)).strftime("%Y-%m-%d")
    fig_line_reg_recap.add_vline(
        x=data_linea,        
        line_width=2.5,
        line_dash="dash", 
        line_color="black",
    )

    fig_line_reg_recap.add_annotation(
        x=data_linea,
        y=1,               # Posiziona il testo in alto rispetto al grafico
        yref="paper",      
        text="Soglia oltre la 5° settimana",
        showarrow=False,
        font=dict(color="black", size=14),
        xanchor="left",         # Aggancia il testo alla coordinata X partendo dalla sua sinistra
        xshift=5
    )

    return fig_line_reg_recap


@app_risultati.callback(
    Output("datatable-reg-recap-ag", "rowData"),
    Output("datatable-reg-recap-ag", "columnDefs"),
    Input("regione-filter", "value"),
    Input("recap-filter", "value"),
    Input("url", "pathname")
)
def update_table_reg_recap_ag(regioni_sel, recap_sel, pathname):
    from .models import view_tabella_sintesi_reg_recap
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    tab_sintesi_reg_recap = view_tabella_sintesi_reg_recap.objects.filter(SIMULAZIONE_ID = id_simulazione, REGIONE__in = regioni_sel, UNIFIED_DELIVERY_DRIVER__in = recap_sel).order_by(F('SUM_COUNT_REQUEST_5_SETT').desc(nulls_last=True)).values()
    df_tab_sintesi_reg_recap = pd.DataFrame(tab_sintesi_reg_recap)
    df_tab_sintesi_reg_recap.drop(columns=["id","SIMULAZIONE_ID_id","PROVINCE","UNIFIED_DELIVERY_DRIVER"], axis=1, inplace=True)
    colonne_ordinate = ['REGIONE', 'PROVINCIA_RECAPITISTA', 'SUM_COUNT_REQUEST', 'AVG_COUNT_REQUEST', 'SUM_COUNT_REQUEST_5_SETT', 'SUM_COUNT_RESIDUI']
    df_tab_sintesi_reg_recap = df_tab_sintesi_reg_recap[colonne_ordinate]
    df_tab_sintesi_reg_recap = df_tab_sintesi_reg_recap.rename(columns={"REGIONE": "Regione","PROVINCIA_RECAPITISTA": "Provincia - Recapitista" , "SUM_COUNT_REQUEST": "Volumi Totali", "AVG_COUNT_REQUEST": "Volumi Medi Settimanali", "SUM_COUNT_REQUEST_5_SETT": "Volumi Oltre la 5° Settimana", "SUM_COUNT_RESIDUI": "Volumi Residui"})
    rowData = df_tab_sintesi_reg_recap.to_dict("records")
    columnDefs = []
    for i in df_tab_sintesi_reg_recap.columns:
        column_spec = {"field": i}
        if i not in ("Regione", "Provincia - Recapitista", "Volumi Medi Settimanali"):
            column_spec["valueFormatter"] = {"function": "d3.format(',')(params.value)"}
        # Esempio: se la colonna è numerica, formatta con 1 decimale
        if i == "Volumi Medi Settimanali":
            column_spec["valueFormatter"] = {"function": "d3.format(',.1f')(params.value)"}
        columnDefs.append(column_spec)
    return rowData, columnDefs

@app_risultati.callback(
    Output("datatable-reg-recap-ag", "exportDataAsCsv"),
    Input("btn-export-reg-recap", "n_clicks"),
    prevent_initial_call=True,
)
def export_data_reg_recap(n_clicks):
    if n_clicks > 0:
        return True
    return False



@app_risultati.callback(
    Output("area-plot", "figure"),
    Input("url", "pathname")
)
def update_chart_regioni_recap(pathname):
    from .models import table_output_grafico_reg_recap
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    recap = table_output_grafico_reg_recap.objects.filter(SIMULAZIONE_ID = id_simulazione).values()
    df_recap = pd.DataFrame(recap)
    df_recap = df_recap.groupby(["SIMULAZIONE_ID_id","UNIFIED_DELIVERY_DRIVER","SETTIMANA_DELIVERY"])['COUNT_REQUEST'].sum().reset_index()
    # df_regioni_recap["Provincia_Recapitista"] = df_regioni_recap["PROVINCE"] + " - " + df_regioni_recap["UNIFIED_DELIVERY_DRIVER"]
    fig_reg_recap = px.area(
        df_recap,
        x="SETTIMANA_DELIVERY",
        y="COUNT_REQUEST",
        color="UNIFIED_DELIVERY_DRIVER",
        line_group="UNIFIED_DELIVERY_DRIVER",
        markers=True
    )

    fig_reg_recap.update_traces(
        hovertemplate="<br>".join([
            "<b>Recapitista: %{fullData.name}</b>", # Prende il nome dalla traccia (colore)
            "Settimana: %{x|%d-%m-%Y}",
            "Postalizzazioni: <b>%{y:,}</b>",
            "<extra></extra>" # Rimuove il box con il nome della serie a destra
        ])
    )

    fig_reg_recap.update_layout(
        template="none",
        legend=dict(
            title=dict(
                text="Recapitista"
            ),
            x=1.02, 
            y=1, 
            bgcolor="rgba(0,0,0,0)"
        ),
        xaxis=dict(
            title=dict(
                text="Settima di Delivery"
            )
        ),
        yaxis=dict(
            title=dict(
                text="Numero di Postalizzazioni"
            )
        )
    )
    return fig_reg_recap

@app_risultati.callback(
    Output("recap-only-filter", "options"),
    Output("recap-only-filter", "value"),
    Input("url", "pathname")
)
def populate_dropdown_only_recap(pathname):
    from .models import view_output_grafico_mappa_picchi
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    lista_recap = view_output_grafico_mappa_picchi.objects.filter(SIMULAZIONE_ID = id_simulazione).values_list("UNIFIED_DELIVERY_DRIVER", flat=True)
    options = [{"label": r, "value": r} for r in sorted(list(set(lista_recap)))]
    value =sorted(list(set(lista_recap)))[0]
    return options, value

 
@app_risultati.callback(
    Output("map-plot", "figure"),
    Input("recap-only-filter", "value"),
    Input("url", "pathname")
)
def update_map_recap(recap_only_sel, pathname):
    from .models import view_output_grafico_mappa_picchi
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    mappa_picchi = view_output_grafico_mappa_picchi.objects.filter(SIMULAZIONE_ID = id_simulazione, UNIFIED_DELIVERY_DRIVER = recap_only_sel).values()
    df_mappa_picchi = pd.DataFrame(mappa_picchi)

    colori = {
        "No picchi": "green",
        "<50% picchi": "orange",
        ">=50% picchi": "red"
    }

    ## mappa testo->numero per le fasce (adatta se hai altre categorie)
    fascia_to_num = {"No picchi": 0, "<50% picchi": 1, ">=50% picchi": 2}

    # costruisci un colorscale "discreto" che associa range a colori
    # struttura: [ [0.0,color_bassa], [0.3333,color_bassa], [0.3334,color_media], ... ]
    colorscale = [
        [0.0, colori["No picchi"]], [0.3333, colori["No picchi"]],
        [0.3334, colori["<50% picchi"]], [0.6666, colori["<50% picchi"]],
        [0.6667, colori[">=50% picchi"]], [1.0, colori[">=50% picchi"]],
    ]
    df_mappa_picchi["z"] = df_mappa_picchi["FASCIA_PICCO"].map(fascia_to_num)
    fig_picchi = go.Figure()
    fig_picchi = fig_picchi.add_trace(
        go.Choroplethmapbox(
            geojson=geojson,
            locations=df_mappa_picchi["REGIONE"],
            z=df_mappa_picchi["z"],
            featureidkey="properties.reg_name",
            colorscale=colorscale,
            zmin=0, zmax=2,
            marker_opacity=0.6,
            marker_line_width=0,
            name=recap_only_sel,
            #visible=recap_sel,   # mostra solo il primo inizialmente
            showscale=False,
            customdata=df_mappa_picchi[["FASCIA_PICCO"]].values,
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
    Output("datatable-region-ag", "rowData"),
    Output("datatable-region-ag", "columnDefs"),
    Input("recap-only-filter", "value"),
    Input("url", "pathname")
)
def update_table_recap_ag(recap_only_sel,pathname):

    from .models import view_output_tabella_picchi
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    tab_picchi = view_output_tabella_picchi.objects.filter(SIMULAZIONE_ID = id_simulazione, UNIFIED_DELIVERY_DRIVER = recap_only_sel).order_by("REGIONE","PROVINCE").values()
    df_tab_picchi = pd.DataFrame(tab_picchi)

    df_tab_picchi["TOT_PICCO"] = df_tab_picchi["TOT_PICCO"].map({0: 'Assente', 1: 'Presente'})
    df_tab_picchi.drop(columns=["id","SIMULAZIONE_ID"], axis=1, inplace=True)
    df_tab_picchi = df_tab_picchi.rename(columns={"UNIFIED_DELIVERY_DRIVER": "Recapitista", "REGIONE": "Regione" , "PROVINCE": "Provincia", "TOT_PICCO": "Picco"})
    rowData = df_tab_picchi.to_dict("records")
    columnDefs =  [{'field': i} for i in df_tab_picchi.columns]
    return rowData, columnDefs

@app_risultati.callback(
    Output("datatable-region-ag", "exportDataAsCsv"),
    Input("btn-export", "n_clicks"),
    prevent_initial_call=True,
)
def export_data(n_clicks):
    if n_clicks > 0:
        return True
    return False


app_confronto = DjangoDash('dash_confronto_risultati')


# layout
app_confronto.layout = html.Div([
    dcc.Location(id="url", refresh=False), # serve a catturare l'url
    
    html.H3(
        "Simulazione Pianificazione Postalizzazioni per Ente",
        style={"text-align":"center"}),

    html.Div([
        html.Div([
            html.H4(id="titolo-simulazione-1",style={"text-align":"center"})
        ], className="col-sm"),
        html.Div([
            html.H4(id="titolo-simulazione-2",style={"text-align":"center"})
        ], className="col-sm")
    ], className='row text-center'),

    html.Div([
        html.Div([
            html.Div([

                html.Div([

                    html.Label("Seleziona Ente:"),

                    dcc.Dropdown(

                        id="ente-filter-1",

                        options=[],
                        value=[],
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

                        options=[],
                        value=[],
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
            html.H4(id="secondo-titolo-simulazione-1",style={"text-align":"center"})
        ], className="col-sm"),
        html.Div([
            html.H4(id="secondo-titolo-simulazione-2",style={"text-align":"center"})
        ], className="col-sm")
    ], className='row text-center'),

    html.Div([
        html.Div([
            html.Div([

                html.Div([

                    html.Label("Seleziona Regione:"),

                    dcc.Dropdown(

                        id="regione-filter-1",

                        options=[],
                        value=[],
                        multi=True,
                        placeholder="Seleziona una o più regioni..."
                    ),
                ], style={"width": "48%", "display": "inline-block"}
                , className="col-sm"),

                html.Div([

                    html.Label("Seleziona Recapitista:"),

                    dcc.Dropdown(

                        id="recap-filter-1",

                        options=[],
                        value=[],
                        multi=True,
                        placeholder="Seleziona un recapitista..."
                    )
                ], style={"width": "48%", "display": "inline-block"}
                , className="col-sm")
            ], style={"margin-bottom": "10px"}
            , className='row text-center'),
            

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

                        options=[],
                        value=[],
                        multi=True,
                        placeholder="Seleziona una o più regioni..."
                    ),
                ], style={"width": "48%", "display": "inline-block"}
                , className="col-sm"),

                html.Div([

                    html.Label("Seleziona Recapitista:"),

                    dcc.Dropdown(

                        id="recap-filter-2",

                        options=[],
                        value=[],
                        multi=True,
                        placeholder="Seleziona un recapitista..."
                    )
                ], style={"width": "48%", "display": "inline-block"}
                , className="col-sm")
            ], style={"margin-bottom": "10px"}
            , className='row text-center'),

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
            html.H4(id="terzo-titolo-simulazione-1",style={"text-align":"center"})
        ], className="col-sm"),
        html.Div([
            html.H4(id="terzo-titolo-simulazione-2",style={"text-align":"center"})
        ], className="col-sm")
    ], className='row text-center'),

    html.Div([
        html.Div([
            html.Div([

                html.Div([

                    html.Label("Seleziona Recapitista:"),

                    dcc.Dropdown(

                        id="recap-only-filter-1",

                        options=[],
                        value=[],
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
                    data=[],
                    columns=[],
                    #style_as_list_view=True,
                    style_cell={
                        'padding': '5px',
                        'textAlign': 'center',
                        'minWidth': '30px', 'width': '30px', 'maxWidth': '30px',
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

                        options=[],
                        value=[],
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
                    data=[],
                    columns=[],
                    #style_as_list_view=True,
                    style_cell={
                        'padding': '5px',
                        'textAlign': 'center',
                        'minWidth': '30px', 'width': '30px', 'maxWidth': '30px',
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
                    page_size= 10,
                )
            ])
        ], className="col-sm")
    ], className='row text-center'),
])

@app_confronto.callback(
    Output("titolo-simulazione-1", "children"),
    Input("url", "pathname")
)
def aggiorna_da_url(pathname):
    from .models import table_simulazione
    id_simulazione = int(pathname.strip("/").split("/")[-2])
    nome_simulazione = table_simulazione.objects.filter(ID = id_simulazione).values_list("NOME", flat=True)[0]
    titolo = "Risultati Simulazione: "+str(nome_simulazione)
    return titolo

@app_confronto.callback(
    Output("titolo-simulazione-2", "children"),
    Input("url", "pathname")
)
def aggiorna_da_url(pathname):
    from .models import table_simulazione
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    nome_simulazione = table_simulazione.objects.filter(ID = id_simulazione).values_list("NOME", flat=True)[0]
    titolo = "Risultati Simulazione: "+str(nome_simulazione)
    return titolo


@app_confronto.callback(
    Output("ente-filter-1", "options"),
    Output("ente-filter-1", "value"),
    Input("url", "pathname")
)
def populate_dropdown_ente_1(pathname):
    from .models import table_output_grafico_ente
    id_simulazione = int(pathname.strip("/").split("/")[-2])
    lista_enti = table_output_grafico_ente.objects.filter(SIMULAZIONE_ID = id_simulazione).values_list("SENDER_PA_ID", flat=True)
    options = [{"label": r, "value": r} for r in sorted(list(set(lista_enti)))]
    value = list(sorted(list(set(lista_enti))))[:5]
    return options, value

@app_confronto.callback(
    Output("ente-filter-2", "options"),
    Output("ente-filter-2", "value"),
    Input("url", "pathname")
)
def populate_dropdown_ente_2(pathname):
    from .models import table_output_grafico_ente
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    lista_enti = table_output_grafico_ente.objects.filter(SIMULAZIONE_ID = id_simulazione).values_list("SENDER_PA_ID", flat=True)
    options = [{"label": r, "value": r} for r in sorted(list(set(lista_enti)))]
    value = list(sorted(list(set(lista_enti))))[:5]
    return options, value


@app_confronto.callback(
    Output("line-plot-1", "figure"),
    Input("ente-filter-1", "value"),
    Input("url", "pathname")
)
def update_chart_ente_1(ente_sel, pathname):
    from .models import table_output_grafico_ente
    id_simulazione = int(pathname.strip("/").split("/")[-2])
    filtered_enti = table_output_grafico_ente.objects.filter(SIMULAZIONE_ID = id_simulazione, SENDER_PA_ID__in = ente_sel).order_by("SETTIMANA_DELIVERY").values()
    df_filtered_enti = pd.DataFrame(filtered_enti)
    # se non selezionato nulla → grafico vuoto
    if not ente_sel:
        return px.line(title="Nessuna selezione effettuata")

    fig_ente = px.line(
        df_filtered_enti,
        x="SETTIMANA_DELIVERY",
        y="COUNT_REQUEST",
        color='SENDER_PA_ID',
        markers=True
    )

    fig_ente.update_layout(
        legend=dict(
            title=dict(
                text="ID Ente"
            ),
            x=1.02, 
            y=1, 
            bgcolor="rgba(0,0,0,0)"
        ),
        xaxis=dict(
            title=dict(
                text="Settima di Delivery"
            )
        ),
        yaxis=dict(
            title=dict(
                text="Numero di Postalizzazioni"
            )
        )
    )
    return fig_ente

@app_confronto.callback(
    Output("line-plot-2", "figure"),
    Input("ente-filter-2", "value"),
    Input("url", "pathname")
)
def update_chart_ente_2(ente_sel, pathname):
    from .models import table_output_grafico_ente
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    filtered_enti = table_output_grafico_ente.objects.filter(SIMULAZIONE_ID = id_simulazione, SENDER_PA_ID__in = ente_sel).order_by("SETTIMANA_DELIVERY").values()
    df_filtered_enti = pd.DataFrame(filtered_enti)
    # se non selezionato nulla → grafico vuoto
    if not ente_sel:
        return px.line(title="Nessuna selezione effettuata")

    fig_ente = px.line(
        df_filtered_enti,
        x="SETTIMANA_DELIVERY",
        y="COUNT_REQUEST",
        color='SENDER_PA_ID',
        markers=True
    )

    fig_ente.update_layout(
        legend=dict(
            title=dict(
                text="ID Ente"
            ),
            x=1.02, 
            y=1, 
            bgcolor="rgba(0,0,0,0)"
        ),
        xaxis=dict(
            title=dict(
                text="Settima di Delivery"
            )
        ),
        yaxis=dict(
            title=dict(
                text="Numero di Postalizzazioni"
            )
        )
    )
    return fig_ente


@app_confronto.callback(
    Output("secondo-titolo-simulazione-1", "children"),
    Input("url", "pathname")
)
def aggiorna_da_url(pathname):
    from .models import table_simulazione
    id_simulazione = int(pathname.strip("/").split("/")[-2])
    nome_simulazione = table_simulazione.objects.filter(ID = id_simulazione).values_list("NOME", flat=True)[0]
    titolo = "Risultati Simulazione: "+str(nome_simulazione)
    return titolo

@app_confronto.callback(
    Output("secondo-titolo-simulazione-2", "children"),
    Input("url", "pathname")
)
def aggiorna_da_url(pathname):
    from .models import table_simulazione
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    nome_simulazione = table_simulazione.objects.filter(ID = id_simulazione).values_list("NOME", flat=True)[0]
    titolo = "Risultati Simulazione: "+str(nome_simulazione)
    return titolo


@app_confronto.callback(
    Output("regione-filter-1", "options"),
    Output("regione-filter-1", "value"),
    Input("url", "pathname")
)
def populate_dropdown_regione_1(pathname):
    from .models import table_output_grafico_reg_recap
    id_simulazione = int(pathname.strip("/").split("/")[-2])
    lista_regioni = table_output_grafico_reg_recap.objects.filter(SIMULAZIONE_ID = id_simulazione).values_list("REGIONE", flat=True)
    options = [{"label": r, "value": r} for r in sorted(list(set(lista_regioni)))]
    value = sorted(list(set(lista_regioni)))[0]
    return options, value

@app_confronto.callback(
    Output("recap-filter-1", "options"),
    Output("recap-filter-1", "value"),
    Input("url", "pathname")
)
def populate_dropdown_recap_1(pathname):
    from .models import table_output_grafico_reg_recap
    id_simulazione = int(pathname.strip("/").split("/")[-2])
    #df_lista_reg_recap = pd.DataFrame(list(table_output_grafico_reg_recap.objects.filter(SIMULAZIONE_ID = id_simulazione).values()))
    #first_region= sorted(df_lista_reg_recap["REGIONE"].unique())[0]
    lista_recap = table_output_grafico_reg_recap.objects.filter(SIMULAZIONE_ID = id_simulazione).values_list("UNIFIED_DELIVERY_DRIVER", flat=True)
    #options = [{"label": r, "value": r} for r in sorted(list(df_lista_reg_recap["UNIFIED_DELIVERY_DRIVER"].unique()))]
    #value = sorted(list(df_lista_reg_recap.loc[df_lista_reg_recap["REGIONE"] == first_region, "UNIFIED_DELIVERY_DRIVER"].unique()))
    options = [{"label": r, "value": r} for r in sorted(list(set(lista_recap)))]
    value = sorted(list(set(lista_recap)))
    return options, value

@app_confronto.callback(
    Output("regione-filter-2", "options"),
    Output("regione-filter-2", "value"),
    Input("url", "pathname")
)
def populate_dropdown_regione_2(pathname):
    from .models import table_output_grafico_reg_recap
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    lista_regioni = table_output_grafico_reg_recap.objects.filter(SIMULAZIONE_ID = id_simulazione).values_list("REGIONE", flat=True)
    options = [{"label": r, "value": r} for r in sorted(list(set(lista_regioni)))]
    value = sorted(list(set(lista_regioni)))[0]
    return options, value

@app_confronto.callback(
    Output("recap-filter-2", "options"),
    Output("recap-filter-2", "value"),
    Input("url", "pathname")
)
def populate_dropdown_recap_2(pathname):
    from .models import table_output_grafico_reg_recap
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    #df_lista_reg_recap = pd.DataFrame(list(table_output_grafico_reg_recap.objects.filter(SIMULAZIONE_ID = id_simulazione).values()))
    #first_region= sorted(df_lista_reg_recap["REGIONE"].unique())[0]
    lista_recap = table_output_grafico_reg_recap.objects.filter(SIMULAZIONE_ID = id_simulazione).values_list("UNIFIED_DELIVERY_DRIVER", flat=True)
    #options = [{"label": r, "value": r} for r in sorted(list(df_lista_reg_recap["UNIFIED_DELIVERY_DRIVER"].unique()))]
    #value = sorted(list(df_lista_reg_recap.loc[df_lista_reg_recap["REGIONE"] == first_region, "UNIFIED_DELIVERY_DRIVER"].unique()))
    options = [{"label": r, "value": r} for r in sorted(list(set(lista_recap)))]
    value = sorted(list(set(lista_recap)))
    return options, value

@app_confronto.callback(
    Output("area-plot-1", "figure"),
    Input("regione-filter-1", "value"),
    Input("recap-filter-1", "value"),
    Input("url", "pathname")
)
def update_chart_regioni_recap_1(regioni_sel, recap_sel, pathname):
    from .models import table_output_grafico_reg_recap
    id_simulazione = int(pathname.strip("/").split("/")[-2])
    if isinstance(regioni_sel, str):
        regioni_sel = [regioni_sel]

    regioni_recap = table_output_grafico_reg_recap.objects.filter(SIMULAZIONE_ID = id_simulazione, REGIONE__in = regioni_sel, UNIFIED_DELIVERY_DRIVER__in = recap_sel).values()
    # se non selezionato nulla → grafico vuoto
    if not regioni_sel or not recap_sel:
        return px.area(title="Nessuna selezione effettuata")
    df_regioni_recap = pd.DataFrame(regioni_recap)
    # df_regioni_recap["Provincia_Recapitista"] = df_regioni_recap["PROVINCE"] + " - " + df_regioni_recap["UNIFIED_DELIVERY_DRIVER"]
    fig_reg_recap = px.area(
        df_regioni_recap,
        x="SETTIMANA_DELIVERY",
        y="COUNT_REQUEST",
        color="PROVINCIA_RECAPITISTA",
        line_group="PROVINCIA_RECAPITISTA",
        markers=True
    )
    fig_reg_recap.update_layout(
        legend=dict(
            title=dict(
                text="Provincia - Recapitista"
            ),
            x=1.02, 
            y=1, 
            bgcolor="rgba(0,0,0,0)"
        ),
        xaxis=dict(
            title=dict(
                text="Settima di Delivery"
            )
        ),
        yaxis=dict(
            title=dict(
                text="Numero di Postalizzazioni"
            )
        )
    )
    return fig_reg_recap


@app_confronto.callback(
    Output("area-plot-2", "figure"),
    Input("regione-filter-2", "value"),
    Input("recap-filter-2", "value"),
    Input("url", "pathname")
)
def update_chart_regioni_recap_2(regioni_sel, recap_sel, pathname):
    from .models import table_output_grafico_reg_recap
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    if isinstance(regioni_sel, str):
        regioni_sel = [regioni_sel]

    regioni_recap = table_output_grafico_reg_recap.objects.filter(SIMULAZIONE_ID = id_simulazione, REGIONE__in = regioni_sel, UNIFIED_DELIVERY_DRIVER__in = recap_sel).values()
    # se non selezionato nulla → grafico vuoto
    if not regioni_sel or not recap_sel:
        return px.area(title="Nessuna selezione effettuata")
    df_regioni_recap = pd.DataFrame(regioni_recap)
    # df_regioni_recap["Provincia_Recapitista"] = df_regioni_recap["PROVINCE"] + " - " + df_regioni_recap["UNIFIED_DELIVERY_DRIVER"]
    fig_reg_recap = px.area(
        df_regioni_recap,
        x="SETTIMANA_DELIVERY",
        y="COUNT_REQUEST",
        color="PROVINCIA_RECAPITISTA",
        line_group="PROVINCIA_RECAPITISTA",
        markers=True
    )
    fig_reg_recap.update_layout(
        legend=dict(
            title=dict(
                text="Provincia - Recapitista"
            ),
            x=1.02, 
            y=1, 
            bgcolor="rgba(0,0,0,0)"
        ),
        xaxis=dict(
            title=dict(
                text="Settima di Delivery"
            )
        ),
        yaxis=dict(
            title=dict(
                text="Numero di Postalizzazioni"
            )
        )
    )
    return fig_reg_recap
 

@app_confronto.callback(
    Output("terzo-titolo-simulazione-1", "children"),
    Input("url", "pathname")
)
def aggiorna_da_url(pathname):
    from .models import table_simulazione
    id_simulazione = int(pathname.strip("/").split("/")[-2])
    nome_simulazione = table_simulazione.objects.filter(ID = id_simulazione).values_list("NOME", flat=True)[0]
    titolo = "Risultati Simulazione: "+str(nome_simulazione)
    return titolo

@app_confronto.callback(
    Output("terzo-titolo-simulazione-2", "children"),
    Input("url", "pathname")
)
def aggiorna_da_url(pathname):
    from .models import table_simulazione
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    nome_simulazione = table_simulazione.objects.filter(ID = id_simulazione).values_list("NOME", flat=True)[0]
    titolo = "Risultati Simulazione: "+str(nome_simulazione)
    return titolo


@app_confronto.callback(
    Output("recap-only-filter-1", "options"),
    Output("recap-only-filter-1", "value"),
    Input("url", "pathname")
)
def populate_dropdown_only_recap_1(pathname):
    from .models import view_output_grafico_mappa_picchi
    id_simulazione = int(pathname.strip("/").split("/")[-2])
    lista_recap = view_output_grafico_mappa_picchi.objects.filter(SIMULAZIONE_ID = id_simulazione).values_list("UNIFIED_DELIVERY_DRIVER", flat=True)
    options = [{"label": r, "value": r} for r in sorted(list(set(lista_recap)))]
    value =sorted(list(set(lista_recap)))[0]
    return options, value

@app_confronto.callback(
    Output("recap-only-filter-2", "options"),
    Output("recap-only-filter-2", "value"),
    Input("url", "pathname")
)
def populate_dropdown_only_recap_2(pathname):
    from .models import view_output_grafico_mappa_picchi
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    lista_recap = view_output_grafico_mappa_picchi.objects.filter(SIMULAZIONE_ID = id_simulazione).values_list("UNIFIED_DELIVERY_DRIVER", flat=True)
    options = [{"label": r, "value": r} for r in sorted(list(set(lista_recap)))]
    value =sorted(list(set(lista_recap)))[0]
    return options, value

@app_confronto.callback(
    Output("map-plot-1", "figure"),
    Input("recap-only-filter-1", "value"),
    Input("url", "pathname")
)
def update_map_recap_1(recap_only_sel,pathname):
    from .models import view_output_grafico_mappa_picchi
    id_simulazione = int(pathname.strip("/").split("/")[-2])
    mappa_picchi = view_output_grafico_mappa_picchi.objects.filter(SIMULAZIONE_ID = id_simulazione, UNIFIED_DELIVERY_DRIVER = recap_only_sel).values()
    df_mappa_picchi = pd.DataFrame(mappa_picchi)

    # Assegno 3 colori fissi
    colori = {
        "No picchi": "green",
        "<50% picchi": "orange",
        ">=50% picchi": "red"
    }

    ## mappa testo->numero per le fasce (adatta se hai altre categorie)
    fascia_to_num = {"No picchi": 0, "<50% picchi": 1, ">=50% picchi": 2}

    # costruisci un colorscale "discreto" che associa range a colori
    # struttura: [ [0.0,color_bassa], [0.3333,color_bassa], [0.3334,color_media], ... ]
    colorscale = [
        [0.0, colori["No picchi"]], [0.3333, colori["No picchi"]],
        [0.3334, colori["<50% picchi"]], [0.6666, colori["<50% picchi"]],
        [0.6667, colori[">=50% picchi"]], [1.0, colori[">=50% picchi"]],
    ]
    df_mappa_picchi["z"] = df_mappa_picchi["FASCIA_PICCO"].map(fascia_to_num)
    fig_picchi = go.Figure()
    fig_picchi = fig_picchi.add_trace(
        go.Choroplethmapbox(
            geojson=geojson,
            locations=df_mappa_picchi["REGIONE"],
            z=df_mappa_picchi["z"],
            featureidkey="properties.reg_name",
            colorscale=colorscale,
            zmin=0, zmax=2,
            marker_opacity=0.6,
            marker_line_width=0,
            name=recap_only_sel,
            #visible=recap_sel,   # mostra solo il primo inizialmente
            showscale=False,
            customdata=df_mappa_picchi[["FASCIA_PICCO"]].values,
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

@app_confronto.callback(
    Output("datatable-region-1", "data"),
    Output("datatable-region-1", "columns"),
    Input("recap-only-filter-1", "value"),
    Input("url", "pathname")
)
def update_table_recap_1(recap_only_sel, pathname):
    from .models import view_output_tabella_picchi
    id_simulazione = int(pathname.strip("/").split("/")[-2])
    tab_picchi = view_output_tabella_picchi.objects.filter(SIMULAZIONE_ID = id_simulazione, UNIFIED_DELIVERY_DRIVER = recap_only_sel).order_by("REGIONE","PROVINCE").values()
    df_tab_picchi = pd.DataFrame(tab_picchi)

    df_tab_picchi["TOT_PICCO"] = df_tab_picchi["TOT_PICCO"].map({0: 'Assente', 1: 'Presente'})
    df_tab_picchi.drop(columns=["id","SIMULAZIONE_ID"], axis=1, inplace=True)
    df_tab_picchi = df_tab_picchi.rename(columns={"UNIFIED_DELIVERY_DRIVER": "Recapitista", "REGIONE": "Regione" , "PROVINCE": "Provincia", "TOT_PICCO": "Picco"})
    data = df_tab_picchi.to_dict("records")
    columns =  [{'name': i, 'id': i} for i in df_tab_picchi.columns]
    return data, columns

@app_confronto.callback(
    Output("map-plot-2", "figure"),
    Input("recap-only-filter-2", "value"),
    Input("url", "pathname")
)
def update_map_recap_2(recap_only_sel, pathname):
    from .models import view_output_grafico_mappa_picchi
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    mappa_picchi = view_output_grafico_mappa_picchi.objects.filter(SIMULAZIONE_ID = id_simulazione, UNIFIED_DELIVERY_DRIVER = recap_only_sel).values()
    df_mappa_picchi = pd.DataFrame(mappa_picchi)

    # Assegno 3 colori fissi
    colori = {
        "No picchi": "green",
        "<50% picchi": "orange",
        ">=50% picchi": "red"
    }

    ## mappa testo->numero per le fasce (adatta se hai altre categorie)
    fascia_to_num = {"No picchi": 0, "<50% picchi": 1, ">=50% picchi": 2}

    # costruisci un colorscale "discreto" che associa range a colori
    # struttura: [ [0.0,color_bassa], [0.3333,color_bassa], [0.3334,color_media], ... ]
    colorscale = [
        [0.0, colori["No picchi"]], [0.3333, colori["No picchi"]],
        [0.3334, colori["<50% picchi"]], [0.6666, colori["<50% picchi"]],
        [0.6667, colori[">=50% picchi"]], [1.0, colori[">=50% picchi"]],
    ]
    df_mappa_picchi["z"] = df_mappa_picchi["FASCIA_PICCO"].map(fascia_to_num)
    fig_picchi = go.Figure()
    fig_picchi = fig_picchi.add_trace(
        go.Choroplethmapbox(
            geojson=geojson,
            locations=df_mappa_picchi["REGIONE"],
            z=df_mappa_picchi["z"],
            featureidkey="properties.reg_name",
            colorscale=colorscale,
            zmin=0, zmax=2,
            marker_opacity=0.6,
            marker_line_width=0,
            name=recap_only_sel,
            #visible=recap_sel,   # mostra solo il primo inizialmente
            showscale=False,
            customdata=df_mappa_picchi[["FASCIA_PICCO"]].values,
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

@app_confronto.callback(
    Output("datatable-region-2", "data"),
    Output("datatable-region-2", "columns"),
    Input("recap-only-filter-2", "value"),
    Input("url", "pathname")
)
def update_table_recap_2(recap_only_sel, pathname):
    from .models import view_output_tabella_picchi
    id_simulazione = int(pathname.strip("/").split("/")[-1])
    tab_picchi = view_output_tabella_picchi.objects.filter(SIMULAZIONE_ID = id_simulazione, UNIFIED_DELIVERY_DRIVER = recap_only_sel).order_by("REGIONE","PROVINCE").values()
    df_tab_picchi = pd.DataFrame(tab_picchi)

    df_tab_picchi["TOT_PICCO"] = df_tab_picchi["TOT_PICCO"].map({0: 'Assente', 1: 'Presente'})
    df_tab_picchi.drop(columns=["id","SIMULAZIONE_ID"], axis=1, inplace=True)
    df_tab_picchi = df_tab_picchi.rename(columns={"UNIFIED_DELIVERY_DRIVER": "Recapitista", "REGIONE": "Regione" , "PROVINCE": "Provincia", "TOT_PICCO": "Picco"})
    data = df_tab_picchi.to_dict("records")
    columns =  [{'name': i, 'id': i} for i in df_tab_picchi.columns]
    return data, columns


