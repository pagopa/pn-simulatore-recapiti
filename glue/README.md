# Glue Jobs - pn-simulatore-recapiti

## Struttura

```
glue/
├── scripts/          # Script Python dei job Glue
├── configs/          # Definizioni dei job (parameters, workers, timeout, ecc.)
└── libs/             # Librerie comuni condivise tra i job
```

## Job disponibili

1. **GenerazioneCsvImportData** - Genera CSV per import data aggregando residui da S3
2. **RecuperoResiduiPaperDelivery** - Recupera residui paper delivery
3. **RecuperoDeclaredCapacityDelta** - Recupera delta declared capacity
4. **RecuperoSenderLimitDelta** - Recupera delta sender limit
5. **RecuperoRisultatiPaperDelivery** - Recupera risultati paper delivery

## Deploy

### Automatic Deploy (Raccomandato)

Il deploy avviene automaticamente insieme al microservizio tramite CloudFormation:

1. **Build CI/CD**: 
   - Ogni commit su `develop`/`main` triggera CodeBuild
   - CodeBuild esegue `buildspec-lambdas-glue.yml`
   - Build delle Lambda functions in `fuctions/`
   - Upload degli script Glue su S3: `s3://aws-glue-assets-{account}-{region}/scripts/pn-simulatore-recapiti/`
   - Upload delle librerie su S3: `s3://aws-glue-assets-{account}-{region}/libs/pn-simulatore-recapiti/`

2. **Deploy CloudFormation**:
   - Il template `scripts/aws/cfn/microservice.yml` viene deployato
   - Crea automaticamente tutti i 5 job Glue con le configurazioni corrette
   - Crea il ruolo IAM `GlueServiceRole` con i permessi necessari
   - Configura i job con i parametri del database e S3 bucket

### Manual Deploy (Solo per test)

Se necessario per test locali:

```bash
cd glue/scripts
./deploy-glue-jobs.sh <environment> <aws-account-id> <region>

# Example for dev environment
./deploy-glue-jobs.sh dev 830192246553 eu-south-1
```

### Prerequisiti

- Glue Connection già creata: `pn-simulatore-recapiti-glue-network-connection`
- S3 bucket: `aws-glue-assets-{account}-{region}` (creato automaticamente da AWS)
- Database Aurora PostgreSQL deployato tramite `scripts/aws/cfn/storage.yml`
- Secrets Manager con credenziali database

### Parametri comuni

- `--jdbc_connection`: Connection string per Aurora PostgreSQL
- `--s3_bucket`: Bucket S3 per i dati
- `--secretsManager_SecretId`: Secret ARN per le credenziali DB
- `--mese_simulazione`: Mese di riferimento per la simulazione (formato: YYYY-MM-DD)

## Connessione

Tutti i job usano la connessione `pn-simulatore-recapiti-glue-network-connection` per accedere al database Aurora in VPC.

## Note

- Glue version: 5.0
- Worker type: G.1X (2 workers)
- Timeout: 480 minuti
- Max concurrent runs: 1
