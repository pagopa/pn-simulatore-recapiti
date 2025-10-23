from django.urls import path
from Simulatore.views import *

urlpatterns = [
    path('risultati/<id_simulazione>', risultati, name='risultati'),
    path('confronto_risultati/<id_simulazione>', confronto_risultati, name='confronto_risultati'),
]