from django.urls import path
from Simulatore.views import *

urlpatterns = [
    path('risultati/<id_simulazione>', risultati, name='risultati'),
    path('confronto_risultati/<id1>/<id2>/', confronto_risultati, name='confronto_risultati'),
]