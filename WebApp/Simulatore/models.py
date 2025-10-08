from django.db import models
from django.contrib.auth.models import AbstractUser

class CustomUser(AbstractUser):
    email = models.EmailField(unique=True)

class table_simulazione(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    NOME = models.CharField(max_length=50, null=True)
    DESCRIZIONE = models.TextField(null=True)
    UTENTE_ID = models.ForeignKey(CustomUser, db_column='UTENTE_ID', on_delete=models.CASCADE, null=True)
    STATO = models.CharField(max_length=20, null=True)
    TIMESTAMP_ESECUZIONE = models.DateTimeField(null=True)
    class Meta:
        db_table = 'SIMULAZIONE'
        indexes = [
            models.Index(fields=['STATO'], name='INDICE_STATO')
        ]

class table_capacita_modificate(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    MESE_SIMULAZIONE = models.CharField(max_length=20, null=True)
    TIPO_CAPACITA = models.CharField(max_length=25, null=True)
    RECAPITISTA = models.CharField(max_length=50, null=True)
    REGIONE = models.CharField(max_length=50, null=True)
    PROVINCIA = models.CharField(max_length=50, null=True)
    POSTALIZZAZIONI = models.IntegerField(null=True)
    CAPACITA = models.IntegerField(null=True)
    ACTIVATION_DATE_FROM = models.DateField(null=True)
    ACTIVATION_DATE_TO = models.DateField(null=True)
    SIMULAZIONE_ID = models.ForeignKey(table_simulazione, db_column='SIMULAZIONE_ID', on_delete=models.CASCADE, null=True)
    class Meta:
        db_table = 'CAPACITA_MODIFICATE'