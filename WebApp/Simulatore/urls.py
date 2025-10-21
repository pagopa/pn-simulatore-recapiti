from django.urls import path
from Simulatore.views import *

urlpatterns = [
    path('', dashboard_view, name='dashboard'),
]