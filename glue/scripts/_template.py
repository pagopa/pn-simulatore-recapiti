# Template per nuovi script Glue
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Parametri obbligatori
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Inizializzazione
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

##################################
# Il tuo codice qui
##################################

# TODO: Implementa la logica del job

##################################
# Fine job
##################################

# Commit finale per indicare SUCCESS
job.commit()
