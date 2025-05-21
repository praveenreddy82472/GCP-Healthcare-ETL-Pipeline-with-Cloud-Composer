python my_pipeline3.py \
  --runner DataflowRunner \
  --project=project3cloudcompser \
  --region=us-central1 \
  --stagingLocation=gs://composer18/staging \
  --tempLocation=gs://composer18/temp \
  --templateLocation=gs://composer18/templates/health_template \
  --input=gs://composer18/healthcare_dataset.csv \
  --output=project3cloudcompser:healthdataset.health_etl




gcloud dataflow jobs run health-data-etl-$(date +%Y%m%d%H%M%S) \
  --gcs-location=gs://composer18/templates/health_template \
  --region=us-central1 \
  --parameters input=gs://composer18/healthcare_dataset.csv,output=project3cloudcompser:healthdataset.health_etl
