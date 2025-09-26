from django.shortcuts import render

import plotly.graph_objects as go
import json
import plotly.utils as putils

def homepage(request):
    lista_simulazioni = [
        {
            'id': '0001',
            'timestamp_esecuzione': '2025/08/10 17:00:00',
            'stato': 'Schedulata',
            'utente': 'Paolo Bianchi',
            'nome_simulazione': 'Test 4',
            'descrizione': 'Test con leggero aumento delle capacità',
            'errore': None,
            'durata': None,
        },
        {
            'id': '0002',
            'timestamp_esecuzione': '2025/07/23 15:00:00',
            'stato': 'In lavorazione',
            'utente': 'Mario Rossi',
            'nome_simulazione': 'Test 3',
            'descrizione': 'Test con leggera diminuzione delle capacità',
            'errore': None,
            'durata': None,
        },
        {
            'id': '0003',
            'timestamp_esecuzione': '2025/07/23 10:00:00',
            'stato': 'Lavorata',
            'utente': 'Mario Rossi',
            'nome_simulazione': 'Test 2',
            'descrizione': 'Test con drastico aumento delle capacità',
            'errore': None,
            'durata': '0:13:50'
        },
        {
            'id': '0004',
            'timestamp_esecuzione': '2025/07/23 10:00:00',
            'stato': 'Lavorata',
            'utente': '',
            'nome_simulazione': 'Automatizzata',
            'descrizione': 'Pianificazione settimanale automatizzata',
            'errore': None,
            'durata': '0:13:50'
        },
        {
            'id': '0005',
            'timestamp_esecuzione': '2025/07/22 11:30:00',
            'stato': 'Non completata',
            'utente': 'Luca Neri',
            'nome_simulazione': 'Test 1',
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

    x = [1, 2, 3, 4]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=x, y=[1, 4, 9, 16]))
    fig.add_trace(go.Bar(x=x, y=[6, -8, -4.5, 8]))
    fig.add_trace(go.Bar(x=x, y=[-15, -3, 4.5, -8]))
    fig.add_trace(go.Bar(x=x, y=[-1, 3, -3, -4]))

    fig.update_layout(barmode='relative', title_text='Relative Barmode')

    fig_for_visualizzation = json.dumps(fig, cls=putils.PlotlyJSONEncoder)

    context = {
        'fig_for_visualizzation': fig_for_visualizzation
    }
    return render(request, "simulazioni/risultati.html", context)



def calendario(request):
    return render(request, "calendario/calendario.html")

def bozze(request):
    lista_bozze = [
        {
            'timestamp_scheduling': '2025/07/23 15:00:00',
            'nome_simulazione': 'Test 3',
            'utente': 'Mario Rossi',
            'descrizione': 'Test 5'
        },
        {
            'timestamp_scheduling': '2025/07/23 10:00:00',
            'nome_simulazione': 'Test 2',
            'utente': 'Mario Rossi',
            'descrizione': 'Test 2'
        },
        {
            'timestamp_scheduling': '2025/07/22 11:30:00',
            'nome_simulazione': 'Test 1',
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