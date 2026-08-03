#!/usr/bin/env bash
# Deploy the pipeline to Cloud Run.
# Usage: ./deploy.sh [PROJECT_ID] [REGION]

set -euo pipefail

PROJECT="${1:-$(gcloud config get-value project)}"
REGION="${2:-southamerica-east1}"
SERVICE="familia-pipeline"
IMAGE="gcr.io/${PROJECT}/${SERVICE}"
GCS_BUCKET_AUDIOS="${GCS_BUCKET_AUDIOS:-libro-familiar-audios}"
GCS_BUCKET_FOTOS="${GCS_BUCKET_FOTOS:-libro-familiar-fotos}"
GCS_BUCKET_LIBROS="${GCS_BUCKET_LIBROS:-libro-familiar-libros}"
SA="familia-pipeline@${PROJECT}.iam.gserviceaccount.com"
CLOUD_TASKS_QUEUE="pipeline-jobs"
CLOUD_TASKS_LOCATION="${REGION}"

echo "Project        : ${PROJECT}"
echo "Region         : ${REGION}"
echo "Image          : ${IMAGE}"
echo "Bucket audios  : ${GCS_BUCKET_AUDIOS}"
echo "Bucket fotos   : ${GCS_BUCKET_FOTOS}"
echo "Bucket libros  : ${GCS_BUCKET_LIBROS}"
echo "Tasks queue    : ${CLOUD_TASKS_QUEUE} (${CLOUD_TASKS_LOCATION})"
echo ""

# ── GCS buckets ─────────────────────────────────────────────────────────────
for BUCKET in "${GCS_BUCKET_AUDIOS}" "${GCS_BUCKET_FOTOS}" "${GCS_BUCKET_LIBROS}"; do
  if ! gsutil ls -b "gs://${BUCKET}" &>/dev/null; then
    echo "Creando bucket gs://${BUCKET}..."
    gsutil mb -p "${PROJECT}" -l "${REGION}" "gs://${BUCKET}"
    gsutil iam ch "serviceAccount:${SA}:objectAdmin" "gs://${BUCKET}"
    echo "Bucket creado."
  else
    echo "Bucket gs://${BUCKET} ya existe."
  fi
done
echo ""

# ── Cloud Tasks queue (idempotente) ─────────────────────────────────────────
gcloud tasks queues create "${CLOUD_TASKS_QUEUE}" \
  --location="${CLOUD_TASKS_LOCATION}" \
  --max-concurrent-dispatches=5 \
  --max-attempts=3 \
  --project="${PROJECT}" 2>/dev/null || echo "Queue ${CLOUD_TASKS_QUEUE} ya existe."

gcloud tasks queues add-iam-policy-binding "${CLOUD_TASKS_QUEUE}" \
  --location="${CLOUD_TASKS_LOCATION}" \
  --member="serviceAccount:${SA}" \
  --role="roles/cloudtasks.enqueuer" \
  --project="${PROJECT}" > /dev/null

# ── Cloud Run URL (necesaria para que Cloud Tasks apunte al worker) ──────────
CLOUD_RUN_URL=$(gcloud run services describe "${SERVICE}" \
  --project="${PROJECT}" \
  --region="${REGION}" \
  --format="value(status.url)" 2>/dev/null || \
  echo "https://familia-pipeline-776445604502.${REGION}.run.app")
echo "Cloud Run URL  : ${CLOUD_RUN_URL}"
echo ""

# Build and push via Cloud Build
gcloud builds submit \
  --project="${PROJECT}" \
  --tag="${IMAGE}" \
  .

# Sheet IDs (hardcoded in sheets.py, listed here for reference)
# SHEET_ID (Respuestas + Perfiles): 1A1M79ITLeRVWkwct7pqjUTmLu9NWXn9uDLpKWMMomgM
# FAMILIA_SHEET_ID (Integrantes + Relaciones): 1iEpnly_f3OQL6nLH41XU76zg1iM2vHZQyQdF0RLVQFE

# Deploy to Cloud Run
gcloud run deploy "${SERVICE}" \
  --project="${PROJECT}" \
  --image="${IMAGE}" \
  --region="${REGION}" \
  --platform=managed \
  --allow-unauthenticated \
  --memory=2Gi \
  --cpu=2 \
  --timeout=3600 \
  --no-cpu-throttling \
  --service-account="${SA}" \
  --set-env-vars="GCS_BUCKET_AUDIOS=${GCS_BUCKET_AUDIOS},GCS_BUCKET_FOTOS=${GCS_BUCKET_FOTOS},GCS_BUCKET_LIBROS=${GCS_BUCKET_LIBROS},FONTS_DIR=/app/fonts,FIRESTORE_PROJECT_ID=${PROJECT},CLOUD_TASKS_QUEUE=${CLOUD_TASKS_QUEUE},CLOUD_TASKS_LOCATION=${CLOUD_TASKS_LOCATION},CLOUD_RUN_URL=${CLOUD_RUN_URL},BASE_URL=https://www.ethosbios.com" \
  --set-secrets="ANTHROPIC_API_KEY=ANTHROPIC_API_KEY:latest,OPENAI_API_KEY=OPENAI_API_KEY:latest,GCP_SA_KEY_JSON=GOOGLE_CREDENTIALS:latest,POSTMARK_API_KEY=POSTMARK_API_KEY:latest,ADMIN_PASSWORD=ADMIN_PASSWORD:latest,SESSION_SECRET=SESSION_SECRET:latest,STRIPE_WEBHOOK_SECRET=STRIPE_WEBHOOK_SECRET:latest,MP_ACCESS_TOKEN=MP_ACCESS_TOKEN:latest,HOTMART_HOTTOK=HOTMART_HOTTOK:latest"

echo ""
echo "Deploy complete."
gcloud run services describe "${SERVICE}" \
  --project="${PROJECT}" \
  --region="${REGION}" \
  --format="value(status.url)"
