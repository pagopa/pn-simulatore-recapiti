from django.db import models
from django_pgviews import view as pg

# necessario per impostare sul db il tipo timestamp without time zone per DatetimeField
class NaiveDateTimeField(models.DateTimeField):
    def db_type(self, connection):
        return 'timestamp without time zone'


class table_simulazione(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    NOME = models.CharField(max_length=50, null=True)
    DESCRIZIONE = models.TextField(null=True)
    STATO = models.CharField(max_length=20, null=True) # [Lavorata, In lavorazione, Schedulata, Non completata, Bozza]
    TRIGGER = models.CharField(max_length=10, null=True) # [Schedule, Now]
    TIMESTAMP_ESECUZIONE = NaiveDateTimeField(null=True) # formato YYYY-MM-DD HH:mm:ss
    MESE_SIMULAZIONE = models.CharField(max_length=20, null=True)# formato YYYY-MM
    TIPO_CAPACITA = models.CharField(max_length=25, null=True)  # [BAU, Picco, Combinata, Produzione] -> Per le automatizzate settiamo "Produzione"
    TIPO_SIMULAZIONE = models.CharField(max_length=25, null=True) # [Manuale, Automatizzata]
    class Meta:
        db_table = 'SIMULAZIONE'
        indexes = [
            models.Index(fields=['MESE_SIMULAZIONE'], name='indice_mese_simulazione'),
            models.Index(fields=['TIMESTAMP_ESECUZIONE'], name='indice_timestamp_esecuzione'),
        ]

class table_capacita_simulate(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    UNIFIED_DELIVERY_DRIVER = models.CharField(max_length=80, null=True)
    ACTIVATION_DATE_FROM = NaiveDateTimeField(null=True)
    ACTIVATION_DATE_TO = NaiveDateTimeField(null=True)
    CAPACITY = models.IntegerField(null=True)
    SUM_MONTHLY_ESTIMATE = models.IntegerField(null=True)
    SUM_WEEKLY_ESTIMATE = models.IntegerField(null=True)
    REGIONE = models.CharField(max_length=50, null=True)
    COD_SIGLA_PROVINCIA = models.CharField(max_length=5, null=True)
    PRODUCT_890 = models.BooleanField(null=True)
    PRODUCT_AR = models.BooleanField(null=True)
    LAST_UPDATE_TIMESTAMP = NaiveDateTimeField(null=True)
    FLAG_DEFAULT = models.BooleanField(null=True)
    SIMULAZIONE_ID = models.ForeignKey(table_simulazione, db_column='SIMULAZIONE_ID', on_delete=models.CASCADE, null=True)
    class Meta:
        db_table = 'CAPACITA_SIMULATE'
        indexes = [
            models.Index(fields=['SIMULAZIONE_ID'], name='indice_simulazione_id'),
            models.Index(fields=['ACTIVATION_DATE_FROM'], name='indice_activation_date_from'),
            models.Index(fields=['ACTIVATION_DATE_TO'], name='indice_activation_date_to'),
            models.Index(fields=['COD_SIGLA_PROVINCIA'], name='indice_cod_sigla_provincia'),
            models.Index(fields=['CAPACITY'], name='indice_capacity'),
            models.Index(fields=['LAST_UPDATE_TIMESTAMP'], name='indice_last_update_timestamp_3')
        ]
    '''
    FLAG_DEFAULT: serve per tracciare le modifiche dell'utente alle capacità reali tramite la WebApp
    Regole FLAG_DEFAULT:
        - per tipo capacità bau, settiamo FLAG_DEFAULT a False solo per recapitista-provincia-settimana in cui l'utente modifica la capacità
        - per tipo capacità picco, settiamo FLAG_DEFAULT a False
        - per tipo capacità combinata, settiamo FLAG_DEFAULT a False solo per recapitista-provincia-settimana in cui l'utente modifica la capacità
        - per le capacità con ACTIVATION_DATE_TO NULL, settiamo False solo per tipo capacità picco
        - altrimenti True
    '''
    
class table_capacita_simulate_delta(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    UNIFIED_DELIVERY_DRIVER = models.CharField(max_length=80, null=True)
    ACTIVATION_DATE_FROM = NaiveDateTimeField(null=True)
    ACTIVATION_DATE_TO = NaiveDateTimeField(null=True)
    CAPACITY = models.IntegerField(null=True)
    SUM_MONTHLY_ESTIMATE = models.IntegerField(null=True)
    SUM_WEEKLY_ESTIMATE = models.IntegerField(null=True)
    REGIONE = models.CharField(max_length=50, null=True)
    COD_SIGLA_PROVINCIA = models.CharField(max_length=5, null=True)
    PRODUCT_890 = models.BooleanField(null=True)
    PRODUCT_AR = models.BooleanField(null=True)
    LAST_UPDATE_TIMESTAMP = NaiveDateTimeField(null=True)
    FLAG_DEFAULT = models.BooleanField(null=True)
    SIMULAZIONE_ID = models.IntegerField(null=True)
    class Meta:
        db_table = 'CAPACITA_SIMULATE_DELTA'


class table_declared_capacity(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    CAPACITY = models.IntegerField(null=True)
    GEOKEY = models.CharField(max_length=5, null=True)
    TENDER_ID_GEOKEY = models.CharField(max_length=11, null=True)
    PRODUCT_890 = models.BooleanField(null=True)
    PRODUCT_AR = models.BooleanField(null=True)
    PRODUCT_RS = models.BooleanField(null=True)
    TENDER_ID = models.CharField(max_length=8, null=True)
    UNIFIED_DELIVERY_DRIVER = models.CharField(max_length=80, null=True)
    CREATED_AT = NaiveDateTimeField(null=True)
    PEAK_CAPACITY = models.IntegerField(null=True)
    ACTIVATION_DATE_FROM = NaiveDateTimeField(null=True)
    ACTIVATION_DATE_TO = NaiveDateTimeField(null=True)
    PK = models.CharField(max_length=100, null=True)
    PRODUCTION_CAPACITY = models.IntegerField(null=True)
    LAST_UPDATE_TIMESTAMP = NaiveDateTimeField(null=True)
    class Meta:
        db_table = 'DECLARED_CAPACITY'
        indexes = [
            models.Index(fields=['GEOKEY'], name='indice_geokey'),
            models.Index(fields=['ACTIVATION_DATE_FROM'], name='indice_activation_date_from_2'),
            models.Index(fields=['ACTIVATION_DATE_TO'], name='indice_activation_date_to_2'),
            models.Index(fields=['PK'], name='indice_pk'),
            models.Index(fields=['LAST_UPDATE_TIMESTAMP'], name='indice_last_update_timestamp'),
        ]

class table_declared_capacity_delta(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    CAPACITY = models.IntegerField(null=True)
    GEOKEY = models.CharField(max_length=5, null=True)
    TENDER_ID_GEOKEY = models.CharField(max_length=11, null=True)
    PRODUCT_890 = models.BooleanField(null=True)
    PRODUCT_AR = models.BooleanField(null=True)
    PRODUCT_RS = models.BooleanField(null=True)
    TENDER_ID = models.CharField(max_length=8, null=True)
    UNIFIED_DELIVERY_DRIVER = models.CharField(max_length=80, null=True)
    CREATED_AT = NaiveDateTimeField(null=True)
    PEAK_CAPACITY = models.IntegerField(null=True)
    ACTIVATION_DATE_FROM = NaiveDateTimeField(null=True)
    ACTIVATION_DATE_TO = NaiveDateTimeField(null=True)
    PK = models.CharField(max_length=100, null=True)
    PRODUCTION_CAPACITY = models.IntegerField(null=True)
    LAST_UPDATE_TIMESTAMP = NaiveDateTimeField(null=True)
    class Meta:
        db_table = 'DECLARED_CAPACITY_DELTA'

class table_sender_limit(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    PK = models.CharField(max_length=80, null=True)
    DELIVERY_DATE = models.DateField(null=True)
    WEEKLY_ESTIMATE = models.IntegerField(null=True)
    MONTHLY_ESTIMATE = models.IntegerField(null=True)
    ORIGINAL_ESTIMATE = models.IntegerField(null=True)
    PA_ID = models.CharField(max_length=80, null=True)
    PRODUCT_TYPE = models.CharField(max_length=3, null=True)
    PROVINCE = models.CharField(max_length=5, null=True)
    LAST_UPDATE_TIMESTAMP = NaiveDateTimeField(null=True)
    class Meta:
        db_table = 'SENDER_LIMIT'
        indexes = [
            models.Index(fields=['DELIVERY_DATE'], name='indice_delivery_date'),
            models.Index(fields=['PROVINCE'], name='indice_province'),
            models.Index(fields=['PK'], name='indice_pk_2'),
            models.Index(fields=['LAST_UPDATE_TIMESTAMP'], name='indice_last_update_timestamp_2')
        ]

class table_sender_limit_delta(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    PK = models.CharField(max_length=80, null=True)
    DELIVERY_DATE = models.DateField(null=True)
    WEEKLY_ESTIMATE = models.IntegerField(null=True)
    MONTHLY_ESTIMATE = models.IntegerField(null=True)
    ORIGINAL_ESTIMATE = models.IntegerField(null=True)
    PA_ID = models.CharField(max_length=80, null=True)
    PRODUCT_TYPE = models.CharField(max_length=3, null=True)
    PROVINCE = models.CharField(max_length=5, null=True)
    LAST_UPDATE_TIMESTAMP = NaiveDateTimeField(null=True)
    class Meta:
        db_table = 'SENDER_LIMIT_DELTA'

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
        indexes = [
            models.Index(fields=['PROVINCIA'], name='indice_provincia'),
            models.Index(fields=['COD_SIGLA_PROVINCIA'], name='indice_cod_sigla_provincia_2')
        ]

class table_output_grafico_ente(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    SIMULAZIONE_ID = models.ForeignKey(table_simulazione, db_column='SIMULAZIONE_ID', on_delete=models.CASCADE, null=True)
    SENDER_PA_ID = models.CharField(max_length=80, null=True)
    SETTIMANA_DELIVERY = NaiveDateTimeField(null=True)
    COUNT_REQUEST = models.IntegerField(null=True)
    class Meta:
        db_table = 'OUTPUT_GRAFICO_ENTE'
        indexes = [
            models.Index(fields=['SIMULAZIONE_ID'], name='indice_simulazione_id_2'),
            models.Index(fields=['SENDER_PA_ID'], name='indice_sender_pa_id'),
            models.Index(fields=['SETTIMANA_DELIVERY'], name='indice_settimana_delivery_2')
        ]

class table_output_grafico_reg_recap(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    SIMULAZIONE_ID = models.ForeignKey(table_simulazione, db_column='SIMULAZIONE_ID', on_delete=models.CASCADE, null=True)
    PROVINCE = models.CharField(max_length=5, null=True)
    REGIONE = models.CharField(max_length=50, null=True)
    UNIFIED_DELIVERY_DRIVER = models.CharField(max_length=80, null=True)
    SETTIMANA_DELIVERY = NaiveDateTimeField(null=True)
    PROVINCIA_RECAPITISTA = models.CharField(max_length=100, null=True)
    COUNT_REQUEST = models.IntegerField(null=True)
    class Meta:
        db_table = 'OUTPUT_GRAFICO_REG_RECAP'
        indexes = [
            models.Index(fields=['SIMULAZIONE_ID'], name='indice_simulazione_id_3'),
            models.Index(fields=['PROVINCE'], name='indice_province_2'),
            models.Index(fields=['SETTIMANA_DELIVERY'], name='indice_settimana_delivery'),
            models.Index(fields=['COUNT_REQUEST'], name='indice_count_request')
        ]

class table_output_residui_ente(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    SIMULAZIONE_ID = models.ForeignKey(table_simulazione, db_column='SIMULAZIONE_ID', on_delete=models.CASCADE, null=True)
    SENDER_PA_ID = models.CharField(max_length=80, null=True)
    COUNT_RESIDUI = models.IntegerField(null=True)
    class Meta:
        db_table = 'OUTPUT_RESIDUI_ENTE'
        indexes = [
            models.Index(fields=['SIMULAZIONE_ID'], name='indice_simulazione_id_5'),
            models.Index(fields=['SENDER_PA_ID'], name='indice_sender_pa_id_2'),
        ]

class table_capacita_simulate_cap(models.Model):
    ID = models.AutoField(primary_key=True, unique=True)
    UNIFIED_DELIVERY_DRIVER = models.CharField(max_length=80, null=True)
    ACTIVATION_DATE_FROM = NaiveDateTimeField(null=True)
    ACTIVATION_DATE_TO = NaiveDateTimeField(null=True)
    CAPACITY = models.IntegerField(null=True)
    PEAK_CAPACITY = models.IntegerField(null=True)
    GEOKEY = models.CharField(max_length=5, null=True)
    LAST_UPDATE_TIMESTAMP = NaiveDateTimeField(null=True)
    SIMULAZIONE_ID = models.ForeignKey(table_simulazione, db_column='SIMULAZIONE_ID', on_delete=models.CASCADE, null=True)
    class Meta:
        db_table = 'CAPACITA_SIMULATE_CAP'
        indexes = [
            models.Index(fields=['SIMULAZIONE_ID'], name='indice_simulazione_id_4')
        ]


# VISTA output_capacity_setting
class view_output_capacity_setting(pg.View):
    id = models.AutoField(primary_key=True)
    UNIFIED_DELIVERY_DRIVER = models.CharField(max_length=80, null=True)
    ACTIVATION_DATE_FROM = NaiveDateTimeField(null=True)
    ACTIVATION_DATE_TO = NaiveDateTimeField(null=True)
    CAPACITY = models.IntegerField(null=True)
    PEAK_CAPACITY = models.IntegerField(null=True)
    PRODUCTION_CAPACITY = models.IntegerField(null=True)
    SUM_WEEKLY_ESTIMATE = models.IntegerField(null=True)
    SUM_MONTHLY_ESTIMATE = models.IntegerField(null=True)
    REGIONE = models.CharField(max_length=50, null=True)
    COD_SIGLA_PROVINCIA = models.CharField(max_length=5, null=True)
    PROVINCIA = models.CharField(max_length=50, null=True)
    PRODUCT_890 = models.BooleanField(null=True)
    PRODUCT_AR = models.BooleanField(null=True)
    MONTH_DELIVERY = models.SmallIntegerField(null=True)

    sql = """
    WITH "SENDERLIMIT_BY_MONTH" AS (
            SELECT DISTINCT ON ("PA_ID","PRODUCT_TYPE", "DELIVERY_DATE","PROVINCE")
                "DELIVERY_DATE","WEEKLY_ESTIMATE", "MONTHLY_ESTIMATE", "PA_ID", "PRODUCT_TYPE", "PROVINCE"
            FROM public."SENDER_LIMIT"
        ),
        "SUM_SENDERLIMIT_BY_MONTH" AS (
            SELECT "DELIVERY_DATE", "PRODUCT_TYPE", "PROVINCE", SUM("WEEKLY_ESTIMATE") AS "SUM_WEEKLY_ESTIMATE", SUM("MONTHLY_ESTIMATE") AS "SUM_MONTHLY_ESTIMATE"
            FROM "SENDERLIMIT_BY_MONTH"
            GROUP BY "DELIVERY_DATE", "PRODUCT_TYPE", "PROVINCE"
        ),
        "PROV_REG" AS (
            SELECT DISTINCT ON ("COD_SIGLA_PROVINCIA") "PROVINCIA","REGIONE","COD_SIGLA_PROVINCIA"
            FROM public."CAP_PROV_REG"
    ),
        "FILTERED_CAPACITY_BY_PRODUCT" AS (
            SELECT
                public."DECLARED_CAPACITY"."UNIFIED_DELIVERY_DRIVER",
                public."DECLARED_CAPACITY"."ACTIVATION_DATE_FROM",
                public."DECLARED_CAPACITY"."ACTIVATION_DATE_TO",
                public."DECLARED_CAPACITY"."CAPACITY",
                public."DECLARED_CAPACITY"."PEAK_CAPACITY",
                public."DECLARED_CAPACITY"."PRODUCTION_CAPACITY",
                "SUM_SENDERLIMIT_BY_MONTH"."SUM_WEEKLY_ESTIMATE",
                "SUM_SENDERLIMIT_BY_MONTH"."SUM_MONTHLY_ESTIMATE",
                "PROV_REG"."REGIONE",
                "PROV_REG"."COD_SIGLA_PROVINCIA",
                "PROV_REG"."PROVINCIA",
                "SUM_SENDERLIMIT_BY_MONTH"."PRODUCT_TYPE",
                public."DECLARED_CAPACITY"."PRODUCT_890",
                public."DECLARED_CAPACITY"."PRODUCT_AR",
                EXTRACT(MONTH FROM "SUM_SENDERLIMIT_BY_MONTH"."DELIVERY_DATE") AS "MONTH_DELIVERY"
            FROM public."DECLARED_CAPACITY"
            LEFT JOIN "PROV_REG"
                ON "PROV_REG"."COD_SIGLA_PROVINCIA" = public."DECLARED_CAPACITY"."GEOKEY"
            INNER JOIN "SUM_SENDERLIMIT_BY_MONTH"
                ON "PROV_REG"."COD_SIGLA_PROVINCIA" = "SUM_SENDERLIMIT_BY_MONTH"."PROVINCE"
                AND (
        public."DECLARED_CAPACITY"."ACTIVATION_DATE_FROM" >= 
        ("SUM_SENDERLIMIT_BY_MONTH"."DELIVERY_DATE" + ((1 - EXTRACT(DOW FROM  "SUM_SENDERLIMIT_BY_MONTH"."DELIVERY_DATE")::int + 7) % 7) * INTERVAL '1 day')
        AND 
        public."DECLARED_CAPACITY"."ACTIVATION_DATE_FROM" <= 
        (("SUM_SENDERLIMIT_BY_MONTH"."DELIVERY_DATE" + INTERVAL '1 month') + ((1 - EXTRACT(DOW FROM "SUM_SENDERLIMIT_BY_MONTH"."DELIVERY_DATE" + INTERVAL '1 month')::int + 7) % 7) * INTERVAL '1 day')
        )
            WHERE ("SUM_SENDERLIMIT_BY_MONTH"."PRODUCT_TYPE"='890' AND public."DECLARED_CAPACITY"."PRODUCT_890"=true)
                OR ("SUM_SENDERLIMIT_BY_MONTH"."PRODUCT_TYPE"='AR' AND public."DECLARED_CAPACITY"."PRODUCT_AR"=true)
        )
        SELECT
            ROW_NUMBER() OVER () AS id,  
            "UNIFIED_DELIVERY_DRIVER",
            "ACTIVATION_DATE_FROM",
            "ACTIVATION_DATE_TO",
            "CAPACITY",
            "PEAK_CAPACITY",
            "PRODUCTION_CAPACITY",
            SUM("SUM_WEEKLY_ESTIMATE") AS "SUM_WEEKLY_ESTIMATE",
            SUM("SUM_MONTHLY_ESTIMATE") AS "SUM_MONTHLY_ESTIMATE",
            "REGIONE",
            "PROVINCIA",
            "COD_SIGLA_PROVINCIA",
            "PRODUCT_890",
            "PRODUCT_AR",
            "MONTH_DELIVERY"
        FROM "FILTERED_CAPACITY_BY_PRODUCT"
        GROUP BY "UNIFIED_DELIVERY_DRIVER","COD_SIGLA_PROVINCIA","MONTH_DELIVERY","ACTIVATION_DATE_FROM","ACTIVATION_DATE_TO","CAPACITY","PEAK_CAPACITY","PRODUCTION_CAPACITY","REGIONE","PROVINCIA","PRODUCT_890","PRODUCT_AR"
    """

    class Meta:
        db_table = 'output_capacity_setting'
        managed = False



# VISTA output_modified_capacity_setting
class view_output_modified_capacity_setting(pg.View):
    id = models.AutoField(primary_key=True)
    UNIFIED_DELIVERY_DRIVER = models.CharField(max_length=80, null=True)
    ACTIVATION_DATE_FROM = NaiveDateTimeField(null=True)
    ACTIVATION_DATE_TO = NaiveDateTimeField(null=True)
    CAPACITY = models.IntegerField(null=True) # contiene la capacità reale di bau
    MODIFIED_CAPACITY = models.IntegerField(null=True) # contiene la capacità specificata dall'utente tramite il modulo della Nuova simulazione
    PEAK_CAPACITY = models.IntegerField(null=True) # contiene la capacità reale di picco
    PRODUCTION_CAPACITY = models.IntegerField(null=True)
    SUM_WEEKLY_ESTIMATE = models.IntegerField(null=True)
    SUM_MONTHLY_ESTIMATE = models.IntegerField(null=True)
    REGIONE = models.CharField(max_length=50, null=True)
    COD_SIGLA_PROVINCIA = models.CharField(max_length=5, null=True)
    PROVINCIA = models.CharField(max_length=50, null=True)
    PRODUCT_890 = models.BooleanField(null=True)
    PRODUCT_AR = models.BooleanField(null=True)
    MONTH_DELIVERY = models.SmallIntegerField(null=True)
    SIMULAZIONE_ID = models.IntegerField(null=True)

    sql = """
        SELECT
            ROW_NUMBER() OVER () AS id,
            public."CAPACITA_SIMULATE"."UNIFIED_DELIVERY_DRIVER",         
            CASE 
                WHEN public."CAPACITA_SIMULATE"."ACTIVATION_DATE_FROM"::date = public."output_capacity_setting"."ACTIVATION_DATE_FROM"::date THEN public."output_capacity_setting"."ACTIVATION_DATE_FROM"
                ELSE public."CAPACITA_SIMULATE"."ACTIVATION_DATE_FROM"
            END AS "ACTIVATION_DATE_FROM",
            CASE 
                WHEN public."CAPACITA_SIMULATE"."ACTIVATION_DATE_TO"::date = public."output_capacity_setting"."ACTIVATION_DATE_TO"::date THEN public."output_capacity_setting"."ACTIVATION_DATE_TO"
                ELSE public."CAPACITA_SIMULATE"."ACTIVATION_DATE_TO"
            END AS "ACTIVATION_DATE_TO",
            CASE 
                WHEN public."CAPACITA_SIMULATE"."ACTIVATION_DATE_FROM"::date = public."output_capacity_setting"."ACTIVATION_DATE_FROM"::date THEN public."output_capacity_setting"."CAPACITY"
                ELSE 0
            END AS "CAPACITY",
            public."CAPACITA_SIMULATE"."CAPACITY" AS "MODIFIED_CAPACITY",
            CASE 
                WHEN public."CAPACITA_SIMULATE"."ACTIVATION_DATE_FROM"::date = public."output_capacity_setting"."ACTIVATION_DATE_FROM"::date THEN public."CAPACITA_SIMULATE"."SUM_WEEKLY_ESTIMATE"
                ELSE 0
            END AS "SUM_WEEKLY_ESTIMATE",
            public."output_capacity_setting"."SUM_MONTHLY_ESTIMATE", 
            public."CAPACITA_SIMULATE"."REGIONE", 
            public."CAPACITA_SIMULATE"."COD_SIGLA_PROVINCIA",
            public."output_capacity_setting"."PROVINCIA",
            public."output_capacity_setting"."PRODUCTION_CAPACITY",
            public."output_capacity_setting"."PEAK_CAPACITY",
            public."CAPACITA_SIMULATE"."PRODUCT_890",
            public."CAPACITA_SIMULATE"."PRODUCT_AR",
            public."output_capacity_setting"."MONTH_DELIVERY",
            public."CAPACITA_SIMULATE"."SIMULAZIONE_ID"
        FROM public."CAPACITA_SIMULATE"
        LEFT JOIN public."SIMULAZIONE"
            ON public."CAPACITA_SIMULATE"."SIMULAZIONE_ID" = public."SIMULAZIONE"."ID"
        LEFT JOIN public."output_capacity_setting"
            ON public."CAPACITA_SIMULATE"."UNIFIED_DELIVERY_DRIVER" = public."output_capacity_setting"."UNIFIED_DELIVERY_DRIVER"
                AND public."CAPACITA_SIMULATE"."ACTIVATION_DATE_FROM"::date = public."output_capacity_setting"."ACTIVATION_DATE_FROM"::date
                AND public."CAPACITA_SIMULATE"."COD_SIGLA_PROVINCIA" = public."output_capacity_setting"."COD_SIGLA_PROVINCIA"
                AND public."CAPACITA_SIMULATE"."PRODUCT_890" = public."output_capacity_setting"."PRODUCT_890"
                AND public."CAPACITA_SIMULATE"."PRODUCT_AR" = public."output_capacity_setting"."PRODUCT_AR"
                AND public."CAPACITA_SIMULATE"."SUM_MONTHLY_ESTIMATE" = public."output_capacity_setting"."SUM_MONTHLY_ESTIMATE"
                AND EXTRACT(MONTH FROM CAST(CONCAT("MESE_SIMULAZIONE",'-01') AS DATE)) = public."output_capacity_setting"."MONTH_DELIVERY"
    """

    class Meta:
        db_table = 'output_modified_capacity_setting'
        managed = False


# VISTA output_tabella_picchi
class view_output_tabella_picchi(pg.View):
    id = models.AutoField(primary_key=True)
    SIMULAZIONE_ID = models.IntegerField(null=True)
    PROVINCE = models.CharField(max_length=5, null=True)
    REGIONE = models.CharField(max_length=50, null=True)
    UNIFIED_DELIVERY_DRIVER = models.CharField(max_length=80, null=True)
    TOT_PICCO = models.IntegerField(null=True)

    sql = """
        WITH "flag_picco_prov_recap" AS(
            SELECT
                public."OUTPUT_GRAFICO_REG_RECAP"."SIMULAZIONE_ID", 
                public."OUTPUT_GRAFICO_REG_RECAP"."PROVINCE",
                public."OUTPUT_GRAFICO_REG_RECAP"."REGIONE",
                public."OUTPUT_GRAFICO_REG_RECAP"."UNIFIED_DELIVERY_DRIVER",
                public."OUTPUT_GRAFICO_REG_RECAP"."SETTIMANA_DELIVERY",
                CASE 
                    WHEN public."OUTPUT_GRAFICO_REG_RECAP"."COUNT_REQUEST" >= public."CAPACITA_SIMULATE"."CAPACITY"
                        THEN 1
                        ELSE 0
                END AS "FLAG_PICCO"
            FROM public."OUTPUT_GRAFICO_REG_RECAP"
            LEFT JOIN public."CAPACITA_SIMULATE"
                ON public."OUTPUT_GRAFICO_REG_RECAP"."SIMULAZIONE_ID" = public."CAPACITA_SIMULATE"."SIMULAZIONE_ID"
                AND public."OUTPUT_GRAFICO_REG_RECAP"."UNIFIED_DELIVERY_DRIVER" = public."CAPACITA_SIMULATE"."UNIFIED_DELIVERY_DRIVER"
                AND public."OUTPUT_GRAFICO_REG_RECAP"."PROVINCE" = public."CAPACITA_SIMULATE"."COD_SIGLA_PROVINCIA"
                AND public."OUTPUT_GRAFICO_REG_RECAP"."SETTIMANA_DELIVERY" =  public."CAPACITA_SIMULATE"."ACTIVATION_DATE_FROM"
        )
        SELECT 
            ROW_NUMBER() OVER () AS id,
            "SIMULAZIONE_ID","PROVINCE","REGIONE", "UNIFIED_DELIVERY_DRIVER", 
            CASE 
                WHEN SUM("FLAG_PICCO") > 0 
                    THEN 1
                ELSE 0 
            END AS "TOT_PICCO"
        FROM "flag_picco_prov_recap"
        GROUP BY ("SIMULAZIONE_ID","PROVINCE","REGIONE", "UNIFIED_DELIVERY_DRIVER")
    """

    class Meta:
        db_table = 'output_tabella_picchi'
        managed = False


# VISTA output_grafico_mappa_picchi
class view_output_grafico_mappa_picchi(pg.View):
    id = models.AutoField(primary_key=True)
    SIMULAZIONE_ID = models.IntegerField(null=True)
    REGIONE = models.CharField(max_length=50, null=True)
    UNIFIED_DELIVERY_DRIVER = models.CharField(max_length=80, null=True)
    PROP_PICCO = models.DecimalField(max_digits=6, decimal_places=4, null=True)
    FASCIA_PICCO = models.CharField(max_length=50, null=True)

    sql = """
        WITH "count_prov_recap" AS (
            SELECT 
                "SIMULAZIONE_ID","REGIONE", "UNIFIED_DELIVERY_DRIVER",
                SUM("TOT_PICCO") AS "TOT_PICCO_REG",
                COUNT(DISTINCT "PROVINCE") AS "COUNT_PROV"
            FROM public."output_tabella_picchi"
            GROUP BY ("SIMULAZIONE_ID","REGIONE", "UNIFIED_DELIVERY_DRIVER")
        )
        SELECT 
            ROW_NUMBER() OVER () AS id,
            *,
            "TOT_PICCO_REG" * 1.0 /"COUNT_PROV" AS "PROP_PICCO",
            CASE 
                WHEN "TOT_PICCO_REG" * 1.0 /"COUNT_PROV" < 0.0001
                    THEN 'No picchi'
                WHEN "TOT_PICCO_REG" * 1.0 /"COUNT_PROV" < 0.5
                    THEN '<50% picchi'
                ELSE '>=50% picchi'
            END AS "FASCIA_PICCO"
        FROM "count_prov_recap"
    """

    class Meta:
        db_table = 'output_grafico_mappa_picchi'
        managed = False


# VISTA view_vista_ente
class view_vista_ente(pg.View):
    id = models.AutoField(primary_key=True)
    DELIVERY_DATE = models.CharField(null=True,max_length=7)
    PA_ID = models.CharField(max_length=80, null=True)
    REGIONE = models.CharField(max_length=50, null=True)
    PRODUCT_TYPE = models.CharField(max_length=3, null=True)
    SUM_MONTHLY_ESTIMATE = models.IntegerField(null=True)

    sql = """
    WITH "CTE_ENTI_REGIONALI" AS
    (
        SELECT
            "PA_ID","REGIONE","PRODUCT_TYPE","DELIVERY_DATE", SUM("MONTHLY_ESTIMATE") AS "SUM_MONTHLY_ESTIMATE"
        FROM public."SENDER_LIMIT"
        LEFT JOIN public."CAP_PROV_REG"
            ON public."CAP_PROV_REG"."COD_SIGLA_PROVINCIA" = public."SENDER_LIMIT"."PROVINCE"
        GROUP BY "PA_ID","REGIONE","PRODUCT_TYPE","DELIVERY_DATE"
    )
    SELECT 
    ROW_NUMBER() OVER () AS id,
    *
    FROM "CTE_ENTI_REGIONALI" 
    WHERE "SUM_MONTHLY_ESTIMATE" > 0
    """

    class Meta:
        db_table = 'vista_ente'
        managed = False


# VISTA view_vista_fornitore
class view_vista_fornitore(pg.View):
    id = models.AutoField(primary_key=True)
    DELIVERY_DATE = models.CharField(null=True,max_length=7)
    UNIFIED_DELIVERY_DRIVER = models.CharField(max_length=80, null=True)
    REGIONE = models.CharField(max_length=50, null=True)
    PRODUCT_TYPE = models.CharField(max_length=3, null=True)
    SUM_MONTHLY_ESTIMATE = models.IntegerField(null=True)

    sql = """
    WITH "CTE_CAPACITY" AS
    (
        SELECT
        "UNIFIED_DELIVERY_DRIVER","GEOKEY","PRODUCT_AR","PRODUCT_890",
                TO_CHAR("ACTIVATION_DATE_FROM", 'YYYY-MM') AS "DELIVERY_DATE"
            FROM public."DECLARED_CAPACITY"
            GROUP BY "UNIFIED_DELIVERY_DRIVER","GEOKEY","PRODUCT_AR","PRODUCT_890","DELIVERY_DATE"
    ),
    "CTE_CAPACITY_TEMP" AS
    (
        SELECT 
        "CTE_CAPACITY"."UNIFIED_DELIVERY_DRIVER", public."CAP_PROV_REG"."REGIONE", public."SENDER_LIMIT"."PRODUCT_TYPE",
        "CTE_CAPACITY"."DELIVERY_DATE", SUM(public."SENDER_LIMIT"."MONTHLY_ESTIMATE") AS "SUM_MONTHLY_ESTIMATE" 
        FROM "CTE_CAPACITY" 
        LEFT JOIN public."CAP_PROV_REG"
            ON public."CAP_PROV_REG"."COD_SIGLA_PROVINCIA" = "CTE_CAPACITY"."GEOKEY"
        LEFT JOIN public."SENDER_LIMIT"
            ON public."SENDER_LIMIT"."PROVINCE" = "CTE_CAPACITY"."GEOKEY"
            AND ((public."SENDER_LIMIT"."PRODUCT_TYPE" = 'AR' and "CTE_CAPACITY"."PRODUCT_AR" = True)
            OR (public."SENDER_LIMIT"."PRODUCT_TYPE" = '890' and "CTE_CAPACITY"."PRODUCT_890" = True))
            AND (TO_CHAR(public."SENDER_LIMIT"."DELIVERY_DATE", 'YYYY-MM') = "CTE_CAPACITY"."DELIVERY_DATE")
        GROUP BY 
        "CTE_CAPACITY"."UNIFIED_DELIVERY_DRIVER",public."CAP_PROV_REG"."REGIONE", public."SENDER_LIMIT"."PRODUCT_TYPE",
        "CTE_CAPACITY"."DELIVERY_DATE"
    )
    SELECT 
    ROW_NUMBER() OVER () AS id,
    *
    FROM "CTE_CAPACITY_TEMP" 
    WHERE "SUM_MONTHLY_ESTIMATE" > 0 
    """

    class Meta:
        db_table = 'vista_fornitore'
        managed = False