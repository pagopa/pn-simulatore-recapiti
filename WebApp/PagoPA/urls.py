from django.urls import path, include
from Simulatore.views import *
from django.http import JsonResponse
from django.conf import settings
from django.conf.urls.static import static

# HEALTH CHECK RESPONSE
def status_view(request):
    return JsonResponse({"status": "ok"}, status=200)

urlpatterns = [
    path('', homepage, name='home'),
    path('calendario', calendario, name='calendario'),

    # SIMULAZIONI
    path('nuova_simulazione/<id_simulazione>', nuova_simulazione, name='nuova_simulazione'),
    #path('risultati/<id_simulazione>', risultati, name='risultati'),
    path('confronto_risultati/<id_simulazione>', confronto_risultati, name='confronto_risultati'),
    path('salva_simulazione', salva_simulazione, name='salva_simulazione'),
    path('rimuovi_simulazione/<id_simulazione>', rimuovi_simulazione, name='rimuovi_simulazione'),

    # BOZZE
    path('bozze', bozze, name='bozze'),

    # AJAX
    path('get_capacita_from_mese_and_tipo_ajax', ajax_get_capacita_from_mese_and_tipo, name="ajax_get_capacita_from_mese_and_tipo"),

    # HEALTH CHECK ALB
    path('status', status_view, name='status'),

    # DASH
    # la dashboard viene generata su http://localhost:8000/django_plotly_dash/app/SimpleExample/ e, poi, viene viene recuperata dalla pagina html http://localhost:8000/dash/
    path('', include('Simulatore.urls')),
    path('django_plotly_dash/', include('django_plotly_dash.urls')),

    # pagine provvisorie per caricamento e rimozione dati db
    path('carica_dati_db', carica_dati_db, name='carica_dati_db'),
    path('rimuovi_dati_db', rimuovi_dati_db, name='rimuovi_dati_db'),
]


# riservato alle pagine d'errore
handler400 = "Simulatore.views.handle_error_400"
handler403 = "Simulatore.views.handle_error_403"
handler404 = "Simulatore.views.handle_error_404"
handler500 = "Simulatore.views.handle_error_500"