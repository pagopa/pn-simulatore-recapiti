# CASI D'USO SIMULATORE

## Fase 1

### CU1.1 - Pianificazione settimanale automatizzata (commesse e capacità di default)
- **Attore principale**: Step Function (Hesplora)  
- **Descrizione**:  
  La Step Function (Hesplora) automaticamente, a cadenza settimanale, recupera la tabella delle commesse a partire dal mese precedente a quello di simulazione, da questo crea un file csv con le spedizioni, e successivamente invoca l’algoritmo di pianificazione (tramite l’operazione RUN_ALGORITHM della Lambda) utilizzando le capacità di produzione (senza modificarle). 
  La Step Function (Hesplora) effettua le seguenti operazioni:
    1. `GET_SENDER_LIMIT` tramite Lambda vengono recuperate le commesse con filtro a partire dalla settimana precedente a quella di inizio simulazione (esempio: nel caso si voglia simulare il mese di Gennaio 2026, la prima settimana di simulazione sarebbe quella del 5 Gennaio, quindi vanno recuperate le commesse a partire dalla settimana precedente ovvero dal 28 Dicembre 2025 in poi).  
    2. `GET_PAPER_DELIVERY` tramite Lambda vengono recuperate le postalizzazioni residue di produzione.
    3. Generazione file csv delle postalizzazioni a partire dalla tabella delle commesse, a cui verranno aggiunte le postalizzazioni residue del mese precedente e quelle dei secondi tentativi/RS in una percentuale fissa (esempio: nel caso si voglia simulare il mese di Gennaio 2026, vanno generate le postalizzazioni a partire dal 28 Dicembre 2025 poiché verranno smaltite a partire dal 5 Gennaio 2026, ovvero con una settimana di delay, a queste vanno aggiunte tutte le postalizzazioni di Dicembre che comunque non saranno smaltite nella prima settimana di Gennaio ovvero i residui, e una percentuale fissa del 5% di secondi tentativi/RS). 
    4. `IMPORT_DATA` tramite Lambda (caricamento csv delle postalizzazioni). 
    5. `RUN_ALGORITHM` tramite Lambda dando in input solo la data di simulazione (esempio: nel caso si voglia simulare il mese di Gennaio 2026, la prima data da inserire sarebbe quella del 5 Gennaio). Questa operazione va ripetuta per tutte le 4 settimane successive (fino al run del 2 Febbraio 2026).
    6. `GET_PAPER_DELIVERY` tramite Lambda vengono recuperate le postalizzazioni residue dell’ultima settimana simulata. Se i residui sono maggiori di 0, effettuo un ulteriore `RUN_ALGORITHM` tramite Lambda della settimana successiva (nel caso di Gennaio 2026, sarà la settimana del 9 Febbraio 2026), e così via finché i residui non saranno smaltiti.
    7. `GET_PAPER_DELIVERY` tramite Lambda vengono recuperate tutte le postalizzazioni programmate attraverso le simulazioni e salvate in un DB PostgresSQL a cui ha accesso la webApp.
    8. `DELETE_DATA` tramite Lambda per eliminare i dati in input della simulazione.

---

### CU1.2 - Simulazione con modifica delle capacità (commesse di default)
- **Attore principale**: Utente (via WebApp Hesplora)  
- **Descrizione**:  
  L’utente avvia una simulazione con l'obiettivo di modificare solo le capacità reali a partire da un mese specifico; queste vengono recuperate dalla WebApp al momento della pianificazione della simulazione tramite API e messe a disposizione dell'utente. In base alle sue scelte viene creata una tabella nel DB (secondo le modalità definite dall’algoritmo) con i dati delle capacità modificate e richiamato il nome della stessa come parametro nella lambda.
  Le postalizzazioni vengono create a partire dalla tabella delle commesse come nel caso CU1.1. 
  La Step Function (Hesplora) effettua le seguenti operazioni:
    1. `GET_SENDER_LIMIT` tramite Lambda vengono recuperate le commesse con filtro a partire dalla settimana precedente a quella di inizio simulazione (esempio: nel caso si voglia simulare il mese di Gennaio 2026, la prima settimana di simulazione sarebbe quella del 5 Gennaio, quindi vanno recuperate le commesse a partire dalla settimana precedente ovvero dal 28 Dicembre 2025 in poi) 
    2. `GET_PAPER_DELIVERY` tramite Lambda vengono recuperate le postalizzazioni residue di produzione.
    3. `GET_DECLARED_CAPACITY` tramite lambda vengono recuperate le capacità di produzione con activationDateFrom più recente.
    4. Generazione tabella aggregata tra capacità di produzione e commesse all’interno del mese (utile per la modifica delle capacità da parte dell’utente)
    5. Dopo che l’utente ha effettuate le modifiche alle capacità, viene generata una tabella ad-hoc nel DB messo a disposizione di Hesplora, che verrà data in input al `RUN_ALGORITHM`.
    6. Generazione file csv delle postalizzazioni a partire dalla tabella delle commesse, a cui verranno aggiunte le postalizzazioni residue del mese precedente e quelle dei secondi tentativi/RS in una percentuale fissa (esempio: nel caso si voglia simulare il mese di Gennaio 2026, vanno generate le postalizzazioni a partire dal 28 Dicembre 2025 poiché verranno smaltite a partire dal 5 Gennaio 2026, ovvero con una settimana di delay, a queste vanno aggiunte tutte le postalizzazioni di Dicembre che comunque non saranno smaltite nella prima settimana di Gennaio ovvero i residui, e una percentuale fissa del 5% di secondi tentativi/RS). 
    7. `IMPORT_DATA` tramite Lambda (caricamento csv delle postalizzazioni). 
    8. `RUN_ALGORITHM` tramite Lambda dando in input la data di simulazione e la tabella delle capacità modificate (esempio: nel caso si voglia simulare il mese di Gennaio 2026, la prima data da inserire sarebbe quella del 5 Gennaio). Questa operazione va ripetuta per tutte le 4 settimane successive (fino al run del 2 Febbraio 2026).
    9. `GET_PAPER_DELIVERY` tramite Lambda vengono recuperate le postalizzazioni residue dell’ultima settimana simulata. Se i residui sono maggiori di 0, effettuo un ulteriore `RUN_ALGORITHM` tramite Lambda della settimana successiva (nel caso di Gennaio 2026, sarà la settimana del 9 Febbraio 2026), e così via finché i residui non saranno smaltiti.
    10. `GET_PAPER_DELIVERY` tramite Lambda vengono recuperate tutte le postalizzazioni programmate attraverso le simulazioni e salvate in un DB PostgresSQL a cui ha accesso la webApp.
    11. `DELETE_DATA` tramite Lambda per eliminare i dati in input della simulazione.

---
---

## Fase 2 [DA RIVEDERE]

### CU2.1 - Simulazione con postalizzazioni mock in più a quelle di commessa (commesse e capacità di default)
- **Attore principale**: Utente (via WebApp Hesplora)  
- **Descrizione**:  
  L’utente avvia una simulazione con l'obiettivo di lanciare l'algoritmo di pianificazione con postalizzazioni mock aggiuntive a quelle di commessa a partire da un mese specifico, attraverso il settaggio di alcuni parametri: prodotto, regione, numero di postalizzazioni.  
  Le commesse e le capacità rimarranno quelle di produzione.  
  La Step Function (Hesplora) effettua le seguenti operazioni:
    1. Genera file csv delle postalizzazioni a partire dalla tabella delle commesse, a cui verranno aggiunte le postalizzazioni dei secondi tentativi/RS in una percentuale fissa.
    2. Genera le postalizzazioni mock aggiuntive e le inserisce nel csv delle postalizzazioni (a queste verranno aggiunte le postalizzazioni dei secondi tentativi/RS in una percentuale fissa).  
    3. `IMPORT_DATA` tramite Lambda (caricamento csv delle postalizzazioni create a partire dalla tabella delle commesse più quelle aggiuntive).  
    4. `RUN_ALGORITHM` tramite Lambda dando in input il mese/data di simulazione, mentre le capacità saranno quelle di produzione.  
    5. `DELETE_DATA` tramite Lambda.  
- **Output**:  
  I risultati vengono raccolti e salvati su un database che poi verrà interrogato dalla webapp.

---

### CU2.2 - Simulazione con modifica delle capacità e postalizzazioni mock (commesse di default)
- **Attore principale**: Utente (via WebApp Hesplora)  
- **Descrizione**:  
  L’utente avvia una simulazione con duplice obiettivo:  
    1. Modificare le capacità reali a partire da un mese specifico; queste vengono recuperate dalla WebApp al momento della pianificazione della simulazione dalla relativa tabella (DA DEFINIRE LE MODALITA' DI RECUPERO CON API LAMBDA) e messe a disposizione dell'utente. In base alle sue scelte viene creata una tabella nel DB (secondo le modalità definite dall’algoritmo) con i dati delle capacità modificate e richiamato il nome della stessa come parametro nella lambda.  
    2. Lanciare l'algoritmo di pianificazione con postalizzazioni mock aggiuntive a quelle di commessa a partire da un mese specifico, attraverso il settaggio di alcuni parametri: prodotto, regione, numero di postalizzazioni.  
  
  Le commesse rimarranno quelle di produzione.  
  La Step Function (Hesplora) effettua le seguenti operazioni:
    1. Genera file csv delle postalizzazioni a partire dalla tabella delle commesse, a cui verranno aggiunte le postalizzazioni dei secondi tentativi/RS in una percentuale fissa.
    2. Genera le postalizzazioni mock aggiuntive e le inserisce nel csv delle postalizzazioni (a queste verranno aggiunte le postalizzazioni dei secondi tentativi/RS in una percentuale fissa).  
    3. `IMPORT_DATA` tramite Lambda (caricamento csv delle postalizzazioni create a partire dalla tabella delle commesse più quelle aggiuntive).  
    4. `RUN_ALGORITHM` tramite Lambda, dando in input il nome della tabella con le capacità modificate ed il mese/data di simulazione. 
    5. `DELETE_DATA` tramite Lambda.  
- **Output**:  
  I risultati vengono raccolti e salvati su un database che poi verrà interrogato dalla webapp.