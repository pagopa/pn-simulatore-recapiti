from django.urls import path, include
from Simulatore.views import *
from django.http import JsonResponse

# HEALTH CHECK RESPONSE
def status_view(request):
    return JsonResponse({"status": "ok"}, status=200)

urlpatterns = [
    path('', homepage, name='home'),
    path('calendario', calendario, name='calendario'),

    # SIMULAZIONI
    path('nuova_simulazione/<id_simulazione>', nuova_simulazione, name='nuova_simulazione'),
    path('salva_simulazione', salva_simulazione, name='salva_simulazione'),
    path('rimuovi_simulazione/<id_simulazione>', rimuovi_simulazione, name='rimuovi_simulazione'),

    # BUTTON DOWNLOAD CSV
    path('download_capacita_per_provincia/<id_simulazione>', download_capacita_per_provincia, name='download_capacita_per_provincia'),
    path('download_capacita_per_cap/<id_simulazione>', download_capacita_per_cap, name='download_capacita_per_cap'),

    # BOZZE
    path('bozze', bozze, name='bozze'),

    # AJAX
    path('recupero_capacita_ajax', ajax_recupero_capacita, name="ajax_recupero_capacita"),
    path('recupero_simulazioni_da_confrontare_ajax', ajax_recupero_simulazioni_da_confrontare, name="ajax_recupero_simulazioni_da_confrontare"),
    path('recupero_province/', recupero_province, name='recupero_province'),

    # HEALTH CHECK ALB
    path('status', status_view, name='status'),

    # DASH
    # la dashboard viene generata su http://localhost:8000/django_plotly_dash/app/SimpleExample/ e, poi, viene viene recuperata dalla pagina html http://localhost:8000/dash/
    path('', include('Simulatore.urls')),
    path('django_plotly_dash/', include('django_plotly_dash.urls')),
]


# riservato alle pagine d'errore
handler400 = "Simulatore.views.handle_error_400"
handler403 = "Simulatore.views.handle_error_403"
handler404 = "Simulatore.views.handle_error_404"
handler500 = "Simulatore.views.handle_error_500"