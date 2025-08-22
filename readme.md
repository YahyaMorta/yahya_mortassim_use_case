# Traitement des Données d'Essais Cliniques

Ce projet contient un DAG [Apache Airflow](https://airflow.apache.org/) qui traite des données issues de plusieurs sources :  
- les **médicaments** (`drugs.csv`),  
- les **publications médicales** (`pubmed.csv`),  
- les **essais cliniques** (`clinical_trials.csv`).  

Le pipeline génère un fichier JSON (au format **JSON Lines**, un objet par ligne) qui relie chaque médicament aux publications et aux essais cliniques dans lesquels il est mentionné, organisés **par journal**.

---

## Choix de Modélisation

Nous avons choisi de **grouper les mentions par journal**.  
Cela permet :  
- de mieux comprendre dans quelles revues scientifiques un médicament est discuté
- de distinguer les mentions provenant de **PubMed** et celles issues des **essais cliniques**
- de faciliter les analyses par revue (impact, nombre de mentions, etc.)

Chaque journal contient donc deux sous-listes :  
1. les mentions issues de **PubMed**  
2. les mentions issues des **essais cliniques**  

Seules les listes non vides sont conservées, pour éviter des structures inutiles comme `[[], []]`.

---

## Structure du Projet

- `dags/` : Définitions de DAG Airflow.  
- `dags/input_files/` : Fichiers de données d’entrée (CSV).  
- `dags/output_files/` : Fichier JSON de sortie (JSON Lines).  
- `src/` : Logique de traitement des données (tâches et helpers).  
- `tests/` : Tests unitaires.  
- `docker-compose.yaml` : Définition des services Airflow (webserver, scheduler, base de données).  
- `.env` : Variables d’environnement pour Docker.
- `src/adhoc.py` : La solution à la question du traitement ad-hoc.

---

## Installation

### Prérequis
- [Docker](https://www.docker.com/products/docker-desktop)

### Démarrage

1. **Cloner le dépôt :**
   ```bash
   git clone https://github.com/votre-nom-d-utilisateur/yahya_mortassim_use_case.git
   cd yahya_mortassim_use_case
   ```

2. **Lancer Airflow :**
   ```bash
   docker-compose up -d
   ```

3. **Accéder à l’UI Airflow :**
   [http://localhost:8080](http://localhost:8080)  
   - utilisateur : `admin`  
   - mot de passe : `admin`  

---

## Déclenchement du DAG

Le DAG principal est **`0_drugs_processing`**. Vous pouvez :  

- le déclencher depuis l’interface Airflow, ou  
- exécuter en CLI :  
  ```bash
  docker-compose run --rm airflow-cli dags trigger 0_drugs_processing
  ```

---

## Tests

Pour exécuter les tests :  
```bash
docker compose exec airflow-scheduler pytest -q tests
```

---

## Architecture et Flux de Données

1. **`read_files_task`**  
   - Lit les fichiers CSV (`drugs`, `pubmed`, `clinical_trials`).  
   - Sérialise les DataFrames en JSON pour passage via XCom.  

2. **`process_data_task`**  
   - Associe chaque médicament aux publications et essais où il est mentionné.  
   - Regroupe les mentions **par journal**, avec deux sous-listes : PubMed et Clinical Trials.  
   - Formate les dates en `JJ/MM/AAAA`.  

3. **`write_results_task`**  
   - Écrit les résultats dans un fichier JSONL (un objet JSON par ligne).  

---

## Exemple de Sortie

Un extrait du fichier JSONL produit :

```json
{"drug": "EPINEPHRINE", "journals": [
  {"name": "Journal of emergency nursing", "mentions": [
    [
      {"clinical_trial_title": "Tranexamic Acid Versus Epinephrine During Exploratory Tympanotomy", "date": "27/04/2020"}
    ]
  ]},
  {"name": "The journal of allergy and clinical immunology. In practice", "mentions": [
    [
      {"pubmed_title": "The High Cost of Epinephrine Autoinjectors and Possible Alternatives.", "date": "02/01/2020"},
      {"pubmed_title": "Time to epinephrine treatment is associated with the risk of mortality...", "date": "03/01/2020"}
    ]
  ]}
]}
```

---

## Améliorations Futures

- **Scalabilité :** utiliser le *dynamic task mapping* pour traiter chaque médicament dans une tâche séparée, ou utiliser une solution de traitement parallèle comme Google Dataflow (MapReduce).  
- **Stockage :** remplacer le stockage local par Google Cloud Storage.  
- **CI/CD :** ajouter un pipeline CI/CD pour automatiser tests et déploiement.  

---

## Traitement Ad-hoc:
Pour lancer le traiement adhoc qui permet d'obtenir le Journal qui mentionne le plus de médicametns:
```bash
docker compose exec airflow-scheduler python -m src.adhoc /opt/airflow/dags/output_files/drugs_file.json
```
