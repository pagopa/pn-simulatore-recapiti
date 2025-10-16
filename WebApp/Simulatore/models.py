from django.db import models
from django_pgviews import view as pg

# necessario per impostare sul db il tipo timestamp without time zone
class NaiveDateTimeField(models.DateTimeField):
    def db_type(self, connection):
        return 'timestamp without time zone'

class table_simulazione(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    NOME = models.CharField(max_length=50, null=True)
    DESCRIZIONE = models.TextField(null=True)
    STATO = models.CharField(max_length=20, null=True) # [Lavorata, In lavorazione, Schedulata, Non completata]
    TRIGGER = models.CharField(max_length=10, null=True) # [Schedule, Now]
    TIMESTAMP_ESECUZIONE = NaiveDateTimeField(null=True)
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
    ACTIVATION_DATE_FROM = NaiveDateTimeField(null=True)
    ACTIVATION_DATE_TO = NaiveDateTimeField(null=True)
    SIMULAZIONE_ID = models.ForeignKey(table_simulazione, db_column='SIMULAZIONE_ID', on_delete=models.CASCADE, null=True)
    class Meta:
        db_table = 'CAPACITA_MODIFICATE'


class table_declared_capacity(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    UNIFIEDDELIVERYDRIVERGEOKEY = models.CharField(max_length=80, null=True)
    DELIVERYDATE = models.DateField(null=True)
    GEOKEY = models.CharField(max_length=5, null=True)
    UNIFIEDDELIVERYDRIVER = models.CharField(max_length=80, null=True)
    USEDCAPACITY = models.IntegerField(null=True)
    CAPACITY = models.IntegerField(null=True)
    class Meta:
        db_table = 'DECLARED_CAPACITY'


class table_sender_limit(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    PK = models.CharField(max_length=80, null=True)
    DELIVERYDATE = models.DateField(null=True)
    WEEKLYESTIMATE = models.IntegerField(null=True)
    MONTHLYESTIMATE = models.IntegerField(null=True)
    ORIGINALESTIMATE = models.IntegerField(null=True)
    PAID = models.CharField(max_length=80, null=True)
    PRODUCTTYPE = models.CharField(max_length=3, null=True)
    PROVINCE = models.CharField(max_length=5, null=True)
    class Meta:
        db_table = 'SENDER_LIMIT'


class table_cap_prov_reg(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    CAP = models.CharField(max_length=5, null=True)
    REGIONE = models.CharField(max_length=50, null=True)
    PROVINCIA = models.CharField(max_length=50, null=True)
    CODSIGLAPROVINCIA = models.CharField(max_length=5, null=True)
    DESCRMACROREGIONE = models.CharField(max_length=50, null=True)
    class Meta:
        db_table = 'CAP_PROV_REG'


# VISTA output_capacity_setting
class table_output_capacity_setting(pg.View):
    id = models.AutoField(primary_key=True)  # Aggiungi questo campo
    UNIFIEDDELIVERYDRIVER = models.CharField(max_length=80, null=True)
    DELIVERYDATE = models.DateField(null=True)
    CAPACITY = models.IntegerField(null=True)
    SUM_WEEKLYESTIMATE = models.IntegerField(null=True)
    SUM_MONTHLYESTIMATE = models.IntegerField(null=True)
    SUM_ORIGINALESTIMATE = models.IntegerField(null=True)
    REGIONE = models.CharField(max_length=50, null=True)
    PROVINCE = models.CharField(max_length=5, null=True)

    sql = """
    WITH "GROUP_BY_SENDERLIMIT" AS (
        SELECT EXTRACT(MONTH FROM "DELIVERYDATE") AS "MONTH_DELIVERY","PROVINCE", SUM("WEEKLYESTIMATE") AS "SUM_WEEKLYESTIMATE", SUM("MONTHLYESTIMATE") AS "SUM_MONTHLYESTIMATE", SUM("ORIGINALESTIMATE") AS "SUM_ORIGINALESTIMATE" 
        FROM public."SENDER_LIMIT" 
        GROUP BY ("PROVINCE",EXTRACT(MONTH FROM "DELIVERYDATE"))
    )
    SELECT 
        ROW_NUMBER() OVER () AS id,  -- E questo
        public."DECLARED_CAPACITY"."UNIFIEDDELIVERYDRIVER", 
        public."DECLARED_CAPACITY"."DELIVERYDATE", 
        public."DECLARED_CAPACITY"."CAPACITY", 
        "GROUP_BY_SENDERLIMIT"."SUM_WEEKLYESTIMATE", 
        "GROUP_BY_SENDERLIMIT"."SUM_MONTHLYESTIMATE", 
        "GROUP_BY_SENDERLIMIT"."SUM_ORIGINALESTIMATE", 
        public."CAP_PROV_REG"."REGIONE", 
        "GROUP_BY_SENDERLIMIT"."PROVINCE"
    FROM public."DECLARED_CAPACITY" 
    LEFT JOIN public."CAP_PROV_REG" 
        ON public."CAP_PROV_REG"."CAP" = public."DECLARED_CAPACITY"."GEOKEY"
    INNER JOIN "GROUP_BY_SENDERLIMIT"
        ON public."CAP_PROV_REG"."CODSIGLAPROVINCIA" = "GROUP_BY_SENDERLIMIT"."PROVINCE"
        AND EXTRACT(MONTH FROM public."DECLARED_CAPACITY"."DELIVERYDATE") = "GROUP_BY_SENDERLIMIT"."MONTH_DELIVERY"
    """

    class Meta:
        db_table = 'output_capacity_setting'
        managed = False
