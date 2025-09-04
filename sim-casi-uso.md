# CASI D'USO SIMULATORE

## Fase 1

### CU1.1 - Pianificazione settimanale automatizzata (commesse e capacità di default)
- **Attore principale**: Step Function (Hesplora)  
- **Descrizione**:  
  La Step Function (Hesplora) invoca automaticamente, a cadenza settimanale, l’algoritmo di pianificazione (tramite l’operazione `RUN_ALGORITHM` della Lambda), utilizzando le capacità e le commesse di produzione (senza modificarle).  
- **Output**:  
  I risultati vengono raccolti e salvati su un database (da definire se dalla Step Function o dalla Lambda).  

---

### CU1.2 - Simulazione con modifica delle capacità (commesse di default)
- **Attore principale**: Utente (via WebApp Hesplora)  
- **Descrizione**:  
  L’utente avvia una simulazione con l'obiettivo di modificare solo le capacità reali sia di BAU che di picco a partire da un mese specifico; queste vengono recuperate dalla WebApp (con frequenza da definire) nella relativa sorgente DynamoDB/S3 e messe a disposizione dell'utente. In base alle sue scelte viene creato un file (secondo le modalità definite dall’algoritmo) con i dati delle capacità modificate che verrà salvato in una specifica tabella/bucket e richiamato nella lambda.  
  Le commesse rimarranno quelle di produzione.  
  La Step Function (Hesplora) effettua le seguenti operazioni:
    1. `RUN_ALGORITHM` tramite Lambda, dando in input il nome della tabella/bucket con le capacità modificate ed il mese/data di simulazione, il resto farà riferimento ai dati di produzione (commesse).  
- **Output**:  
  I risultati vengono raccolti e salvati su un database (da definire se dalla Step Function o dalla Lambda).  

---
---

## Fase 2

### CU2.1 - Simulazione con postalizzazioni mock (commesse e capacità di default)
- **Attore principale**: Utente (via WebApp Hesplora)  
- **Descrizione**:  
  L’utente avvia una simulazione con l'obiettivo di lanciare l'algoritmo di pianificazione con nuove postalizzazioni mock (da capire se esiste un algoritmo interno a PagoPA che generi postalizzazioni mock attraverso il settaggio di alcuni parametri: prodotto, regione, numero di postalizzazione) a partire da un mese specifico.  
  Le commesse e le capacità rimarranno quelle di produzione.  
  La Step Function (Hesplora) effettua le seguenti operazioni:
    1. Genera le postalizzazioni mock (sfruttando o meno algoritmo esistente).  
    2. `IMPORT_DATA` tramite Lambda (caricamento csv delle postalizzazioni mock).  
    3. `RUN_ALGORITHM` tramite Lambda dando in input il mese/data di simulazione, il resto farà riferimento ai dati di produzione (commesse+capacità).  
    4. `DELETE_DATA` tramite Lambda.  
- **Output**:  
  I risultati vengono raccolti e salvati su un database (da definire se dalla Step Function o dalla Lambda).  

---

### CU2.2 - Simulazione con modifica delle capacità e postalizzazioni mock (commesse di default)
- **Attore principale**: Utente (via WebApp Hesplora)  
- **Descrizione**:  
  L’utente avvia una simulazione con duplice obiettivo:  
    1. Modificare le capacità reali sia di BAU che di picco a partire da un mese specifico; queste vengono recuperate dalla WebApp (con frequenza da definire) nella relativa sorgente DynamoDB/S3 e messe a disposizione dell'utente. In base alle sue scelte viene creato un file (secondo le modalità definite dall’algoritmo) con i dati delle capacità modificate che verrà salvato in una specifica tabella/bucket e richiamato nella lambda.      
    2. Lanciare l'algoritmo di pianificazione con nuove postalizzazioni mock (da capire se esiste un algoritmo interno a PagoPA che generi postalizzazioni mock attraverso il settaggio di alcuni parametri: prodotto, regione, numero di postalizzazione).  

  Le commesse rimarranno quelle di produzione.  
  La Step Function (Hesplora) effettua le seguenti operazioni:
    1. Genera le postalizzazioni mock (sfruttando o meno algoritmo esistente).   
    2. `IMPORT_DATA` tramite Lambda (caricamento csv delle postalizzazioni mock).  
    3. `RUN_ALGORITHM` tramite Lambda, dando in input il nome della tabella/bucket con le capacità modificate ed il mese/data di simulazione, il resto farà riferimento ai dati di produzione (commesse). 
    4. `DELETE_DATA` tramite Lambda.  
- **Output**:  
  I risultati vengono raccolti e salvati su un database (da definire se dalla Step Function o dalla Lambda).  