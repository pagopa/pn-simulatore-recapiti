from django.db import models
from django.contrib.auth.models import AbstractUser

class CustomUser(AbstractUser):
    email = models.EmailField(unique=True)

class table_simulazione(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    NOME = models.CharField(max_length=50)
    DESCRIZIONE = models.TextField()
    UTENTE_ID = models.ForeignKey(CustomUser, db_column='UTENTE_ID', on_delete=models.CASCADE, null=True)
    CHOICES_STATO = [
        ("Lavorata", "Lavorata"),
        ("In lavorazione", "In lavorazione"),
        ("Schedulata", "Schedulata"),
        ("Non completata", "Non completata"),
        ("Bozza", "Bozza")
    ]
    STATO = models.CharField(max_length=20, choices=CHOICES_STATO)
    TIMESTAMP_ESECUZIONE = models.DateTimeField()
    class Meta:
        db_table = 'SIMULAZIONE'
        indexes = [
            models.Index(fields=['STATO'], name='INDICE_STATO')
        ]

class table_capacita_modificate(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    MESE_SIMULAZIONE = models.CharField(max_length=20)
    TIPO_CAPACITA = models.CharField(max_length=20)
    RECAPITISTA = models.CharField(max_length=20)
    GEOKEY = models.CharField(max_length=20)
    CAPACITA = models.CharField(max_length=20)
    ACTIVATION_DATE_FROM = models.DateField()
    ACTIVATION_DATE_TO = models.DateField()
    SIMULAZIONE_ID = models.ForeignKey(table_simulazione, db_column='SIMULAZIONE_ID', on_delete=models.CASCADE)
    class Meta:
        db_table = 'CAPACITA_MODIFICATE'