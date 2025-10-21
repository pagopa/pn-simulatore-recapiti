from django.db import models
from django_pgviews import view as pg

# necessario per impostare sul db il tipo timestamp without time zone per DatetimeField
class NaiveDateTimeField(models.DateTimeField):
    def db_type(self, connection):
        return 'timestamp without time zone'

# necessario per impostare sul db il tipo float e non double per FloatField

class table_simulazione(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    NOME = models.CharField(max_length=50, null=True)
    DESCRIZIONE = models.TextField(null=True)
    STATO = models.CharField(max_length=20, null=True) # [Lavorata, In lavorazione, Schedulata, Non completata]
    TRIGGER = models.CharField(max_length=10, null=True) # [Schedule, Now]
    TIMESTAMP_ESECUZIONE = NaiveDateTimeField(null=True)
    MESE_SIMULAZIONE = models.CharField(max_length=20, null=True)
    TIPO_CAPACITA = models.CharField(max_length=25, null=True)
    class Meta:
        db_table = 'SIMULAZIONE'
        indexes = [
            models.Index(fields=['STATO'], name='INDICE_STATO')
        ]

class table_capacita_modificate(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    CAPACITY = models.IntegerField(null=True)
    GEOKEY = models.CharField(max_length=5, null=True)
    TENDERIDGEOKEY = models.CharField(max_length=11, null=True)
    PRODUCT_890 = models.BooleanField(null=True)
    PRODUCT_AR = models.BooleanField(null=True)
    PRODUCT_RS = models.BooleanField(null=True)
    TENDERID = models.CharField(max_length=8, null=True)
    UNIFIEDDELIVERYDRIVER = models.CharField(max_length=80, null=True)
    CREATEDAT = NaiveDateTimeField(null=True)
    PEAKCAPACITY = models.IntegerField(null=True)
    ACTIVATIONDATEFROM = NaiveDateTimeField(null=True)
    ACTIVATIONDATETO = NaiveDateTimeField(null=True)
    PK = models.CharField(max_length=100, null=True)
    SIMULAZIONE_ID = models.ForeignKey(table_simulazione, db_column='SIMULAZIONE_ID', on_delete=models.CASCADE, null=True)
    class Meta:
        db_table = 'CAPACITA_MODIFICATE'


class table_declared_capacity(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    CAPACITY = models.IntegerField(null=True)
    GEOKEY = models.CharField(max_length=5, null=True)
    TENDERIDGEOKEY = models.CharField(max_length=11, null=True)
    PRODUCT_890 = models.BooleanField(null=True)
    PRODUCT_AR = models.BooleanField(null=True)
    PRODUCT_RS = models.BooleanField(null=True)
    TENDERID = models.CharField(max_length=8, null=True)
    UNIFIEDDELIVERYDRIVER = models.CharField(max_length=80, null=True)
    CREATEDAT = NaiveDateTimeField(null=True)
    PEAKCAPACITY = models.IntegerField(null=True)
    ACTIVATIONDATEFROM = NaiveDateTimeField(null=True)
    ACTIVATIONDATETO = NaiveDateTimeField(null=True)
    PK = models.CharField(max_length=100, null=True)
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
    COD_SIGLA_PROVINCIA = models.CharField(max_length=5, null=True)
    POP_CAP = models.IntegerField(null=True)
    PERCENTUALE_POP_CAP = models.DecimalField(max_digits=11, decimal_places=9, null=True)
    class Meta:
        db_table = 'CAP_PROV_REG'


# VISTA output_capacity_setting
class table_output_capacity_setting(pg.View):
    id = models.AutoField(primary_key=True)
    UNIFIEDDELIVERYDRIVER = models.CharField(max_length=80, null=True)
    ACTIVATIONDATEFROM = NaiveDateTimeField(null=True)
    ACTIVATIONDATETO = NaiveDateTimeField(null=True)
    CAPACITY = models.IntegerField(null=True)
    SUM_WEEKLYESTIMATE = models.IntegerField(null=True)
    SUM_MONTHLYESTIMATE = models.IntegerField(null=True)
    REGIONE = models.CharField(max_length=50, null=True)
    PROVINCE = models.CharField(max_length=5, null=True)
    PRODUCT_890 = models.BooleanField(max_length=5, null=True)
    PRODUCT_AR = models.BooleanField(max_length=5, null=True)
    MONTH_DELIVERY = models.SmallIntegerField(null=True)

    sql = """
    WITH "SENDERLIMIT_BY_MONTH" AS (
		SELECT DISTINCT ON ("PAID","PRODUCTTYPE",EXTRACT(MONTH FROM "DELIVERYDATE"),"PROVINCE")
			EXTRACT(MONTH FROM "DELIVERYDATE") AS "MONTH_DELIVERY", "WEEKLYESTIMATE", "MONTHLYESTIMATE", "PAID", "PRODUCTTYPE", "PROVINCE"
		FROM public."SENDER_LIMIT" 
		ORDER BY "PAID", "MONTH_DELIVERY"
	),
	"SUM_SENDERLIMIT_BY_MONTH" AS (
		SELECT "MONTH_DELIVERY", "PRODUCTTYPE", "PROVINCE", SUM("WEEKLYESTIMATE") AS "SUM_WEEKLYESTIMATE", SUM("MONTHLYESTIMATE") AS "SUM_MONTHLYESTIMATE" 
		FROM "SENDERLIMIT_BY_MONTH" 
		GROUP BY "MONTH_DELIVERY", "PRODUCTTYPE", "PROVINCE"
	),
	"PROV_REG" AS (
		SELECT DISTINCT ON ("COD_SIGLA_PROVINCIA") "PROVINCIA","REGIONE","COD_SIGLA_PROVINCIA"
		FROM public."CAP_PROV_REG"
		ORDER BY "COD_SIGLA_PROVINCIA"
	),
	"FILTERED_CAPACITY_BY_PRODUCT" AS (
	    SELECT
	        public."DECLARED_CAPACITY"."UNIFIEDDELIVERYDRIVER", 
	        public."DECLARED_CAPACITY"."ACTIVATIONDATEFROM", 
			public."DECLARED_CAPACITY"."ACTIVATIONDATETO", 
	        public."DECLARED_CAPACITY"."CAPACITY", 
	        "SUM_SENDERLIMIT_BY_MONTH"."SUM_WEEKLYESTIMATE", 
	        "SUM_SENDERLIMIT_BY_MONTH"."SUM_MONTHLYESTIMATE", 
	        "PROV_REG"."REGIONE", 
	        "SUM_SENDERLIMIT_BY_MONTH"."PROVINCE",
			"SUM_SENDERLIMIT_BY_MONTH"."PRODUCTTYPE",
			public."DECLARED_CAPACITY"."PRODUCT_890",
			public."DECLARED_CAPACITY"."PRODUCT_AR",
			"SUM_SENDERLIMIT_BY_MONTH"."MONTH_DELIVERY"
	    FROM public."DECLARED_CAPACITY" 
	    LEFT JOIN "PROV_REG"
	        ON "PROV_REG"."COD_SIGLA_PROVINCIA" = public."DECLARED_CAPACITY"."GEOKEY"
	    INNER JOIN "SUM_SENDERLIMIT_BY_MONTH"
	        ON "PROV_REG"."COD_SIGLA_PROVINCIA" = "SUM_SENDERLIMIT_BY_MONTH"."PROVINCE"
	        AND EXTRACT(MONTH FROM public."DECLARED_CAPACITY"."ACTIVATIONDATEFROM") = "SUM_SENDERLIMIT_BY_MONTH"."MONTH_DELIVERY"
		WHERE ("SUM_SENDERLIMIT_BY_MONTH"."PRODUCTTYPE"='890' AND public."DECLARED_CAPACITY"."PRODUCT_890"=true)
			OR ("SUM_SENDERLIMIT_BY_MONTH"."PRODUCTTYPE"='AR' AND public."DECLARED_CAPACITY"."PRODUCT_AR"=true)
	)
	SELECT 
        ROW_NUMBER() OVER () AS id,  
		"UNIFIEDDELIVERYDRIVER",
		"ACTIVATIONDATEFROM",
		"ACTIVATIONDATETO",
		"CAPACITY",
		SUM("SUM_WEEKLYESTIMATE") AS "SUM_WEEKLYESTIMATE",
		SUM("SUM_MONTHLYESTIMATE") AS "SUM_MONTHLYESTIMATE",
		"REGIONE",
		"PROVINCE",
		"PRODUCT_890",
		"PRODUCT_AR",
		"MONTH_DELIVERY"
	FROM "FILTERED_CAPACITY_BY_PRODUCT"
	GROUP BY "UNIFIEDDELIVERYDRIVER","PROVINCE","MONTH_DELIVERY","ACTIVATIONDATEFROM","ACTIVATIONDATETO","CAPACITY","REGIONE","PRODUCT_890","PRODUCT_AR"
    """

    class Meta:
        db_table = 'output_capacity_setting'
        managed = False