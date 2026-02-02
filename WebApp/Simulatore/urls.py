from django.urls import path
from Simulatore.views import *

urlpatterns = [
    path('risultati/<id_simulazione>', risultati, name='risultati'),
    path('confronto_risultati/<int:id1>/<int:id2>/', confronto_risultati, name='confronto_risultati'),
]