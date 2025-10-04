# Provider Churn Prediction Data Pipeline

This project outlines the design and implementation of a data pipeline for predicting provider churn. It contains data ingestion, data warehouse modeling, a reporting layer, and an AI-ready dataset.

---

## Architecture

The architecture follows a cloud-native pattern using Google Cloud Platform (GCP) services:

SFTP (Simulated) → **Google Cloud Storage** → **Cloud Function** → **BigQuery** (`staging` → `dwh` → `reporting`) → **AI Churn** dataset → **Looker Studio**


## Data Layering

For this exercise, a simplified 4-layer BigQuery structure is used:

1.  **`staging` Dataset**: Receives raw data directly from the ingestion process. Light cleaning and initial transformations (e.g. `order_type` mapping) occur here.
2.  **`dwh` Dataset**: Acts as a data warehouse layer, containing fact and dimension tables. This layer serves both the reporting and AI needs.
    *   *Note on Simplification*: In a production environment, an `intermediate` layer would typically exist between `staging` and `dwh` for more complex transformations, aggregations, and business logic before conformed DWH tables. This was omitted for simplification but is acknowledged as a best practice.
3.  **`ai_churn` Dataset**: A dedicated dataset containing the `ai_churn_training_data` table, specifically created for the churn prediction model.

## Setup and How to Run

### Prerequisites

*   Python 3.8+
*   Google Cloud Project with BigQuery API enabled.
*   `gcloud` CLI installed and authenticated:

    ```bash
    gcloud auth login
    gcloud auth application-default login
    ```
    **Enable Required Services**

    ```bash
    gcloud config set project striking-coil-474009-u5
    gcloud config set functions/region europe-west1
    gcloud services enable \
      cloudfunctions.googleapis.com \
      run.googleapis.com \
      eventarc.googleapis.com \
      bigquery.googleapis.com \
      storage.googleapis.com
    ```

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/scraimundo/churn_model_exercise.git
    cd provider_churn_prediction
    ```

2.  **Create and activate a Python virtual environment:**
    ```bash
    python -m venv venv
    source venv/bin/activate  
    ```

3.  **Install Python dependencies:**
    ```bash
    pip install -r requirements.txt
    ```


### Running the Pipeline

Follow these steps in order to populate the BigQuery datasets:

1.  **Generate Sample Source Files:**
    Navigate to the `sample_data` directory and run the generation script. This creates `payments.csv` and `orders.csv` in `sample_data/raw/`.
    ```bash
    python sample_data/generate_sample_data.py
    ```

2.  **Create BigQuery Staging dataset & tables:**  
    Open the BigQuery SQL editor and run the:
    * `sql/01_staging.sql` – creates `staging.stg_orders`, `staging.stg_payments` (partitioned/clustered).

3.  **Deploy the Cloud Function:**
    From the `ingestion/` folder:
    ```bash
    cd ingestion
    gcloud functions deploy gcs_event_handler \
      --gen2 \
      --runtime=python311 \
      --region=europe-west1 \
      --entry-point=gcs_event_handler \
      --source=. \
      --trigger-bucket=life_file \
      --set-env-vars=GCP_PROJECT_ID=striking-coil-474009-u5,BQ_DATASET=staging,BQ_TEMP_DATASET=staging_temp
      ```
   
    #### Function behavior

    On each new object in `gs://life_file/**`, the function:

    * Loads CSV → a temporary table in `staging_temp.tmp_{payments|orders}_*` 

    * Rounds & casts to NUMERIC and inserts into staging with `load_timestamp = CURRENT_TIMESTAMP()`

    * Derives `subscription_cycle_months` from `order_type` for orders

4.  **Trigger Ingestion:**
    Upload new files to emit finalize events:
    ```bash
    gcloud storage cp sample_data/raw/payments.csv gs://life_file/raw_data/payments_2025-10-03.csv
    gcloud storage cp sample_data/raw/orders.csv   gs://life_file/raw_data/orders_2025-10-03.csv
    ```

5.  **Execute SQL DDL/DML for DWH and Reporting:**
    * #### facts/dims MERGE loads: 
      * Open the BigQuery SQL editor and run the `sql/02_dwh.sql` file

    * #### reporting tables (`provider_segmentation`, `provider_cycle_analysis`)

      * In the BigQuery SQL editor run the `sql/03_reporting_layer.sql` file.

6.  **Execute SQL DDL/DML for AI Churn Dataset:**
    Continue in the BigQuery SQL editor:
    *   `sql/04_ai_churn_ddl.sql`: Creates `ai_churn` dataset and `ai_churn_training_data` table structure.
    *   `sql/05_ai_churn_dml.sql`: Populates the `ai_churn_training_data` table with engineered features and the churn label for a defined `SNAPSHOT_DATE`.


## Deliverables Summary

* `sample_data/generate_sample_data.py` – synthetic orders.csv, payments.csv
* `ingestion/main.py` – Cloud Function 
* `sql/01_staging.sql` – staging tables 
* `sql/02_dwh.sql` – dwh facts/dims + MERGEs
* `sql/03_reporting_layer.sql` – reporting tables for BI
* `sql/04_ai_churn_ddl.sql` – ai_churn DDL 
* `sql/05_ai_churn_dml.sql` – AI training slice 
* `feature_dictionary.md` – feature definitions

## Data Modeling Decisions

*   **Grain**: 
    * `fact_payments`: one row per payment 
    * `fact_orders`: one row per order 
    * `dim_providers`: one row per provider
*   **Keys**: Natural keys (`provider_id`, `order_id`, `payment_id`)
*   **SCDs**: `dim_providers` is simplified to a Type 1-like dimension for this exercise. In a full implementation, Type 2 would be preferred for historical analysis.
*   **Extensibility**: The fact/dimension structure allows for easy addition of new facts or dimensions.
*   **Partitioning/Clustering:** facts partitioned by month on the date and clustered by `provider_id`.
*   **Cycle mapping:** `subscription_cycle_months` derived from `order_type` in ingestion

## Pipeline Robustness 

*   **Idempotency**: `MERGE` loads insert new IDs only
*   **Validation**: payments BIGNUMERIC - rounded NUMERIC. NULL dates filtered
*   **Auditability**: `load_timestamp` in staging tables and `dw_load_timestamp` in DWH tables track when data was processed

## Trade-offs and Assumptions

*   **Simplification**: Many aspects (e.g. full error handling, SCD Type 2, multiple `snapshot_date`s for AI, production-ready CI/CD) were simplified or omitted due to time constraints.
*   **No Live Looker**: `.lkml` files are conceptual; the demo focuses on the underlying SQL views.
*   **PII/PHI**: Assumed no direct PII/PHI in `payments.csv` or `orders.csv` for the exercise. Real data would require specific handling.
*   **Churn**: The rule was created based on orders.