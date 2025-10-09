from django.contrib import admin
from django.urls import path, include
from Simulatore.views import *
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', homepage, name='home'),
    path('calendario', calendario, name='calendario'),

    # SIMULAZIONI
    path('nuova_simulazione/<id_simulazione>', nuova_simulazione, name='nuova_simulazione'),
    path('risultati/<id_simulazione>', risultati, name='risultati'),
    path('confronto_risultati/<id_simulazione>', confronto_risultati, name='confronto_risultati'),
    path('salva_simulazione', salva_simulazione, name='salva_simulazione'),
    path('rimuovi_simulazione/<id_simulazione>', rimuovi_simulazione, name='rimuovi_simulazione'),

    # BOZZE
    path('bozze', bozze, name='bozze'),
    path('rimuovi_bozza/<id_bozza>', rimuovi_bozza, name='rimuovi_bozza'),

    # LOGIN
    path('login_page', login_page, name='login_page'),
    path('login_users/', include('django.contrib.auth.urls')),
    path('login_users/', include('Simulatore.urls')),
]


# riservato alle pagine d'errore
handler400 = "Simulatore.views.handle_error_400"
handler403 = "Simulatore.views.handle_error_403"
handler404 = "Simulatore.views.handle_error_404"
handler500 = "Simulatore.views.handle_error_500"