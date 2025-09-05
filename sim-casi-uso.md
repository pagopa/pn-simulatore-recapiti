# CASI D'USO SIMULATORE

## Fase 1

### CU1.1 - Pianificazione settimanale automatizzata (commesse e capacità di default)
- **Attore principale**: Step Function (Hesplora)  
- **Descrizione**:  
  La Step Function (Hesplora) automaticamente, a cadenza settimanale, recupera la tabella delle commesse (DA DEFINIRE LE MODALITA' DI RECUPERO CON API LAMBDA), da questo crea un file csv con le spedizioni, e successivamente invoca l’algoritmo di pianificazione (tramite l’operazione `RUN_ALGORITHM` della Lambda) utilizzando le capacità di produzione (senza modificarle). 
  La Step Function (Hesplora) effettua le seguenti operazioni:
    1. Genera file csv delle postalizzazioni a partire dalla tabella delle commesse, a cui verranno aggiunte le postalizzazioni dei secondi tentativi/RS in una percentuale fissa.  
    2. `IMPORT_DATA` tramite Lambda (caricamento csv delle postalizzazioni create a partire dalla tabella delle commesse).  
    3. `RUN_ALGORITHM` tramite Lambda dando in input il mese/data di simulazione, mentre le capacità saranno quelle di produzione.  
    4. `DELETE_DATA` tramite Lambda.  
- **Output**:  
  I risultati vengono raccolti e salvati su un database che poi verrà interrogato dalla webapp.

---

### CU1.2 - Simulazione con modifica delle capacità (commesse di default)
- **Attore principale**: Utente (via WebApp Hesplora)  
- **Descrizione**:  
  L’utente avvia una simulazione con l'obiettivo di modificare solo le capacità reali a partire da un mese specifico; queste vengono recuperate dalla WebApp al momento della pianificazione della simulazione dalla relativa tabella (DA DEFINIRE LE MODALITA' DI RECUPERO CON API LAMBDA) e messe a disposizione dell'utente. In base alle sue scelte viene creata una tabella nel DB (secondo le modalità definite dall’algoritmo) con i dati delle capacità modificate e richiamato il nome della stessa come parametro nella lambda.  
  Le postalizzazioni vengono create a partire dalla tabella delle commesse come nel caso CU1.1.
  La Step Function (Hesplora) effettua le seguenti operazioni:
    1. Genera file csv delle postalizzazioni a partire dalla tabella delle commesse, a cui verranno aggiunte le postalizzazioni dei secondi tentativi/RS in una percentuale fissa.
    2. `IMPORT_DATA` tramite Lambda (caricamento csv delle postalizzazioni create a partire dalla tabella delle commesse).  
    3. `RUN_ALGORITHM` tramite Lambda, dando in input il nome della tabella con le capacità modificate ed il mese/data di simulazione.  
    4. `DELETE_DATA` tramite Lambda.  
- **Output**:  
  I risultati vengono raccolti e salvati su un database che poi verrà interrogato dalla webapp.

---
---

## Fase 2

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