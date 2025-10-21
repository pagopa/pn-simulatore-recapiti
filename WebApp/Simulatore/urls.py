from django.urls import path
from Simulatore.views import *

urlpatterns = [
    path('risultati/<id_simulazione>', risultati, name='risultati'),
]