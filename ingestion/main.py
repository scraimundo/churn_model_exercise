import os
import logging
import uuid
from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "striking-coil-474009-u5")
STAGING_DATASET = os.getenv("BQ_DATASET", "staging")
TEMP_DATASET = os.getenv("BQ_TEMP_DATASET", "staging_temp")

PAYMENTS_TABLE = "stg_payments"
ORDERS_TABLE = "stg_orders"

logging.basicConfig(level=logging.INFO)

def _ensure_dataset(client: bigquery.Client, dataset_id: str, location: str = "EU") -> None:
	dsid = f"{client.project}.{dataset_id}"
	ds = bigquery.Dataset(dsid)
	ds.location = location
	client.create_dataset(ds, exists_ok=True)

def _temp_table_ref(client: bigquery.Client, base_name: str) -> bigquery.TableReference:
	_ensure_dataset(client, TEMP_DATASET)
	temp_name = f"{base_name}_{uuid.uuid4().hex[:12]}"
	return bigquery.TableReference(bigquery.DatasetReference(client.project, TEMP_DATASET), temp_name)

def _schema_for(name: str) -> list[bigquery.SchemaField]:
	if name == "payments":
		return [
			bigquery.SchemaField("payment_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("provider_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("payment_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("copay_amount", "BIGNUMERIC"),
            bigquery.SchemaField("insurance_amount", "BIGNUMERIC"),
            bigquery.SchemaField("total_amount", "BIGNUMERIC"),
            bigquery.SchemaField("cost_amount", "BIGNUMERIC"),
            bigquery.SchemaField("profit_amount", "BIGNUMERIC"),
		]
	if name == "orders":
		return [
			bigquery.SchemaField("provider_id", "STRING", mode="REQUIRED"),
			bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
			bigquery.SchemaField("order_date", "DATE", mode="REQUIRED"),
			bigquery.SchemaField("order_type", "STRING"),
		]
	raise ValueError(f"Unknown schema for {name}")

def _target_for_object(obj_name: str) -> tuple[str | None, str | None]:
	lower = obj_name.lower()
	if "payments" in lower:
		return ("payments", PAYMENTS_TABLE)
	if "orders" in lower:
		return ("orders", ORDERS_TABLE)
	return (None, None)

def _load_csv_to_temp(client: bigquery.Client, uri: str, temp_ref: bigquery.TableReference, schema: list[bigquery.SchemaField]):
	job_config = bigquery.LoadJobConfig(
		source_format=bigquery.SourceFormat.CSV,
		skip_leading_rows=1,
		schema=schema,
		write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
	)
	job = client.load_table_from_uri(uri, temp_ref, job_config=job_config)
	result = job.result()
	logging.info("Loaded %s rows into temp %s", result.output_rows, temp_ref)
	return result

def _insert_into_staging_from_temp(client, entity, temp_ref):
	if entity == "payments":
		sql = f"""
		INSERT INTO `{client.project}.{STAGING_DATASET}.{PAYMENTS_TABLE}` (
			payment_id, provider_id, payment_date, copay_amount, insurance_amount,
			total_amount, cost_amount, profit_amount, load_timestamp
		)
		SELECT
			payment_id,
            provider_id,
            payment_date,
            CAST(ROUND(copay_amount, 2)        AS NUMERIC) AS copay_amount,
            CAST(ROUND(insurance_amount, 2)    AS NUMERIC) AS insurance_amount,
            CAST(ROUND(total_amount, 2)        AS NUMERIC) AS total_amount,
            CAST(ROUND(cost_amount, 2)         AS NUMERIC) AS cost_amount,
            CAST(ROUND(profit_amount, 2)       AS NUMERIC) AS profit_amount,
			CURRENT_TIMESTAMP()
		FROM `{temp_ref.project}.{temp_ref.dataset_id}.{temp_ref.table_id}`
		WHERE payment_date IS NOT NULL
		"""
	elif entity == "orders":
		sql = f"""
		INSERT INTO `{client.project}.{STAGING_DATASET}.{ORDERS_TABLE}` (
			provider_id, order_id, order_date, order_type, subscription_cycle_months, load_timestamp
		)
		SELECT
			provider_id,
			order_id,
			order_date,
			order_type,
			CASE
				WHEN order_type LIKE '%3m%' THEN 3
				WHEN order_type LIKE '%6m%' THEN 6
				WHEN order_type LIKE '%9m%' THEN 9
				ELSE 0
			END,
			CURRENT_TIMESTAMP()
		FROM `{temp_ref.project}.{temp_ref.dataset_id}.{temp_ref.table_id}`
		WHERE order_date IS NOT NULL
		"""
	else:
		raise ValueError(f"Unknown entity {entity}")
	client.query(sql).result()
	logging.info("Inserted into staging for %s", entity)

def _drop_table(client: bigquery.Client, ref: bigquery.TableReference) -> None:
	client.delete_table(ref, not_found_ok=True)
	logging.info("Dropped temp table %s", ref)

def gcs_event_handler(event, context) -> None:
	bucket = event.get("bucket")
	name = event.get("name")
	if not bucket or not name:
		logging.info("Missing bucket/name in event; skipping")
		return

	entity, _ = _target_for_object(name)
	if not entity:
		logging.info("No matching entity for object %s; skipping", name)
		return

	uri = f"gs://{bucket}/{name}"
	logging.info("Start ingestion: %s → entity=%s", uri, entity)

	client = bigquery.Client(project=PROJECT_ID)
	_ensure_dataset(client, STAGING_DATASET)
	_ensure_dataset(client, TEMP_DATASET)

	temp_ref = _temp_table_ref(client, f"tmp_{entity}")
	_load_csv_to_temp(client, uri, temp_ref, _schema_for(entity))
	_insert_into_staging_from_temp(client, entity, temp_ref)
	_drop_table(client, temp_ref)

	logging.info("Finished ingestion for %s", uri)

