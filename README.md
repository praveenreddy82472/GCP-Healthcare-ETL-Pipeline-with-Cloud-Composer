# 🏥 GCP Healthcare ETL Pipeline with Cloud Composer

A production-grade ETL pipeline using **Cloud Composer (Apache Airflow)**, **Apache Beam (Dataflow)**, **BigQuery**, and **Google Cloud Storage** to ingest, process, and load healthcare data.

---

## 📌 Overview

This project automates the end-to-end processing of healthcare records stored in a CSV file on Google Cloud Storage:

- 💾 GCS → Read healthcare dataset
- ⚙️ Dataflow → Apache Beam pipeline (batch mode)
- 🧠 BigQuery → Structured healthcare data warehouse
- ⏰ Cloud Composer → Orchestrated with Airflow DAG

---

## ⚙️ Technologies Used

| Service           | Purpose                                 |
|------------------|------------------------------------------|
| Cloud Composer   | Workflow orchestration using Airflow     |
| Apache Beam      | ETL logic and Dataflow template creation |
| Dataflow         | Scalable batch data processing            |
| BigQuery         | Cloud data warehouse                     |
| GCS (Storage)    | CSV source + templates + staging files   |

---

![DataFlow Architecture](https://github.com/praveenreddy82472/tutorial_test/blob/main/dataflow.jpg)

![Airflow Composer](https://github.com/praveenreddy82472/tutorial_test/blob/main/airflow.jpg)

## 🚀 Step-by-Step Setup

### 1️⃣ Upload the CSV to GCS

```bash
gsutil cp data/healthcare_dataset.csv gs://composer18/
### 2️⃣ Build the Dataflow Template
Run the Beam script to generate a reusable Dataflow template:

python pipeline/my_pipeline.py \
  --runner DataflowRunner \
  --project=project3cloudcompser \
  --region=us-central1 \
  --staging_location=gs://streampro18/staging/ \
  --temp_location=gs://streampro18/temp/ \
  --template_location=gs://streampro18/templates/health_template
### 3️⃣ Deploy the Composer DAG
Upload etl_dag.py to the Composer environment DAGs folder:

# Example DAG trigger (via UI or CLI)

parameters={
  'input': 'gs://composer18/healthcare_dataset.csv',
  'output': 'project3cloudcompser:healthdataset.health_etl'
}