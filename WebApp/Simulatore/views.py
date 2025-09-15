from django.shortcuts import render

def homepage(request):
    return render(request, "home.html")

def calendario(request):
    return render(request, "calendario/calendario.html")

def bozze(request):
    return render(request, "bozze/bozze.html")

def nuova_simulazione(request):
    return render(request, "simulazioni/nuova_simulazione.html")

def login(request):
    return render(request, "login.html")