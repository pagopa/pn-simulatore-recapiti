import dash
from dash import dcc, html, Input, Output
from django_plotly_dash import DjangoDash
import plotly.graph_objs as go
import plotly.express as px
import numpy as np
import pandas as pd


app = DjangoDash('SimpleExample')

enti = [
"Regione Lombardia", "INPS", "AMA SPA", "Comune di Roma", "Regione Lazio",
"Poste Italiane", "Agenzia Entrate", "Comune di Milano", "Regione Toscana", "Comune di Napoli"
]

# Simulazione dati per 4 settimane
np.random.seed(42)
dati_enti = []
for ente in enti:
    base = np.random.randint(2000, 8000)  # Valore base per ente
    for settimana in range(1, 5):
        # Fluttuazione con rumore
        valore = base + np.random.randint(-2000, 3000)
        valore = max(500, valore)  # Minimo 500
        dati_enti.append([ente, f"Sett. {settimana}", valore])

# Creazione DataFrame
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

# layout
app.layout = html.Div([
    html.H3("Simulazione Pianificazione Postalizzazioni per Ente"),

    html.Div([

        html.Div([

            html.Label("Seleziona Ente:"),

            dcc.Dropdown(

                id="ente-filter",

                options=[{"label": r, "value": r} for r in sorted(enti)],
                value=list(sorted(enti)),
                multi=True,
                placeholder="Seleziona Ente:"
            ),
        ], style={"display": "inline-block"}),
    ], style={"margin-bottom": "10px"}),

    html.Div([

        dcc.Graph(id="line-plot")

    ]),

    html.H3("Simulazione Pianificazione Postalizzazioni per Provincia e Recapitista"),

    html.Div([

        html.Div([

            html.Label("Seleziona Regione (multi):"),

            dcc.Dropdown(

                id="regione-filter",

                options=[{"label": r, "value": r} for r in sorted(df_regioni_recap["Regione"].unique())],
                value=list(sorted(df_regioni_recap["Regione"].unique())),
                multi=True,
                placeholder="Seleziona le regioni"
            ),
        ], style={"width": "48%", "display": "inline-block"}),

        html.Div([

            html.Label("Seleziona Recapitista (multi):"),

            dcc.Dropdown(

                id="recap-filter",

                options=[{"label": r, "value": r} for r in sorted(recapitisti)],
                value=list(sorted(recapitisti)),
                multi=True,
                placeholder="Seleziona i recapitisti"
            )
        ], style={"width": "48%", "display": "inline-block", "float": "right"})
    ], style={"margin-bottom": "10px"}),

    html.Div([

        dcc.Graph(id="area-plot")

    ])
])

@app.callback(
    Output("line-plot", "figure"),
    Input("ente-filter", "value")
)
def update_chart_ente(ente_sel):
    # se non selezionato nulla → grafico vuoto
    if not ente_sel:
        return px.line(title="Nessuna selezione effettuata")

    df_enti = pd.DataFrame(dati_enti, columns=["Ente", "Settimana", "Postalizzazioni"])

    filtered_enti = df_enti[df_enti["Ente"].isin(ente_sel)]

    fig_ente = px.line(
        filtered_enti,
        x="Settimana",
        y="Postalizzazioni",
        color='Ente',
        markers=True,
        title="Pianificazione Postalizzazioni per Ente"
    )
    fig_ente.update_layout(legend=dict(x=1.02, y=1, bgcolor="rgba(0,0,0,0)"))
    return fig_ente

@app.callback(
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
        markers=True,
        title="Pianificazione Postalizzazioni per Regione e Recapitista"
    )
    fig_reg_recap.update_layout(legend=dict(x=1.02, y=1, bgcolor="rgba(0,0,0,0)"))
    return fig_reg_recap
 



dashboard_name1 = 'dash_example_1'
dash_example1 = DjangoDash(name=dashboard_name1,
                           serve_locally=True,
                           # app_name=app_name
                          )

# Below is a random Dash app.
# I encountered no major problems in using Dash this way. I did encounter problems but it was because
# I was using e.g. Bootstrap inconsistenyly across the dash layout. Staying consistent worked fine for me.
dash_example1.layout = html.Div(id='main',
                                children=[
                                    html.Div([dcc.Dropdown(id='my-dropdown1',
                                                           options=[{'label': 'New York City', 'value': 'NYC'},
                                                                    {'label': 'Montreal', 'value': 'MTL'},
                                                                    {'label': 'San Francisco', 'value': 'SF'}
                                                                   ],
                                                           value='NYC',
                                                           className='col-md-12',
                                                          ),
                                              html.Div(id='test-output-div')
                                             ]),

                                    dcc.Dropdown(
                                        id='my-dropdown2',
                                        options=[
                                            {'label': 'Oranges', 'value': 'Oranges'},
                                            {'label': 'Plums', 'value': 'Plums'},
                                            {'label': 'Peaches', 'value': 'Peaches'}
                                        ],
                                        value='Oranges',
                                        className='col-md-12',
                                    ),

                                    html.Div(id='test-output-div2'),
                                    html.Div(id='test-output-div3')

                                ]) # end of 'main'

@dash_example1.expanded_callback(
    dash.dependencies.Output('test-output-div', 'children'),
    [dash.dependencies.Input('my-dropdown1', 'value')])
def callback_test(*args, **kwargs): #pylint: disable=unused-argument
    'Callback to generate test data on each change of the dropdown'

    # Creating a random Graph from a Plotly example:
    N = 500
    random_x = np.linspace(0, 1, N)
    random_y = np.random.randn(N)

    # Create a trace
    trace = go.Scatter(x=random_x,
                       y=random_y)

    data = [trace]

    layout = dict(title='',
                  yaxis=dict(zeroline=False, title='Total Expense (£)',),
                  xaxis=dict(zeroline=False, title='Date', tickangle=0),
                  margin=dict(t=20, b=50, l=50, r=40),
                  height=350,
                 )


    fig = dict(data=data, layout=layout)
    line_graph = dcc.Graph(id='line-area-graph2', figure=fig, style={'display':'inline-block', 'width':'100%',
                                                                     'height':'100%;'})
    children = [line_graph]

    return children


@dash_example1.expanded_callback(
    dash.dependencies.Output('test-output-div2', 'children'),
    [dash.dependencies.Input('my-dropdown2', 'value')])
def callback_test2(*args, **kwargs):
    'Callback to exercise session functionality'

    children = [html.Div(["You have selected %s." %(args[0])]),
                html.Div(["The session context message is '%s'" %(kwargs['session_state']['django_to_dash_context'])])]

    return children

@dash_example1.expanded_callback(
    [dash.dependencies.Output('test-output-div3', 'children')],
    [dash.dependencies.Input('my-dropdown1', 'value')])
def callback_test(*args, **kwargs): #pylint: disable=unused-argument
    'Callback to generate test data on each change of the dropdown'

    # Creating a random Graph from a Plotly example:
    N = 500
    random_x = np.linspace(0, 1, N)
    random_y = np.random.randn(N)

    # Create a trace
    trace = go.Scatter(x=random_x,
                       y=random_y)

    data = [trace]

    layout = dict(title='',
                  yaxis=dict(zeroline=False, title='Total Expense (£)',),
                  xaxis=dict(zeroline=False, title='Date', tickangle=0),
                  margin=dict(t=20, b=50, l=50, r=40),
                  height=350,
                 )


    fig = dict(data=data, layout=layout)
    line_graph = dcc.Graph(id='line-area-graph2', figure=fig, style={'display':'inline-block', 'width':'100%',
                                                                     'height':'100%;'})
    children = [line_graph]

    return [children]