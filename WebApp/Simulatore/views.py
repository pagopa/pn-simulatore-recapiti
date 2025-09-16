from django.shortcuts import render

def homepage(request):
    lista_simulazioni = [
        {
            'timestamp_esecuzione': '2025/08/10 17:00:00',
            'stato': 'Schedulata',
            'utente': 'Paolo Bianchi',
            'nome_simulazione': 'Test 4'
        },
        {
            'timestamp_esecuzione': '2025/07/23 15:00:00',
            'stato': 'In lavorazione',
            'utente': 'Mario Rossi',
            'nome_simulazione': 'Test 3'
        },
        {
            'timestamp_esecuzione': '2025/07/23 10:00:00',
            'stato': 'Lavorata',
            'utente': 'Mario Rossi',
            'nome_simulazione': 'Test 2'
        },
        {
            'timestamp_esecuzione': '2025/07/22 11:30:00',
            'stato': 'Non completata',
            'utente': 'Luca Neri',
            'nome_simulazione': 'Test 1'
        }
    ]

    context = {
        'lista_simulazioni': lista_simulazioni
    }
    return render(request, "home.html", context)

def calendario(request):
    return render(request, "calendario/calendario.html")

def bozze(request):
    return render(request, "bozze/bozze.html")

def nuova_simulazione(request):
    return render(request, "simulazioni/nuova_simulazione.html")

def login(request):
    return render(request, "login.html")