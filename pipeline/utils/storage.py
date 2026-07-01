"""
GCS utility — replaces drive.py for the Firestore/GCS pipeline.
Handles upload, download and signed URL generation for the three buckets.
"""

import os
import re
import tempfile
from datetime import timedelta

from google.cloud import storage

GCS_BUCKET_AUDIOS = os.environ.get("GCS_BUCKET_AUDIOS", "libro-familiar-audios")
GCS_BUCKET_FOTOS  = os.environ.get("GCS_BUCKET_FOTOS",  "libro-familiar-fotos")
GCS_BUCKET_LIBROS = os.environ.get("GCS_BUCKET_LIBROS", "libro-familiar-libros")

_client = None


def _gcs() -> storage.Client:
    """Return a singleton GCS client."""
    global _client
    if _client is None:
        _client = storage.Client()
    return _client


def _parse_gs_url(gs_url: str) -> tuple[str, str]:
    """gs://bucket/path → (bucket, blob_name). Raises ValueError if it doesn't match."""
    match = re.match(r"^gs://([^/]+)/(.+)$", gs_url)
    if not match:
        raise ValueError(f"URL GCS inválida: {gs_url!r}")
    return match.group(1), match.group(2)


def download_from_gcs(gs_url: str, dest_path: str) -> None:
    """Download the blob to dest_path."""
    bucket_name, blob_name = _parse_gs_url(gs_url)
    bucket = _gcs().bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(dest_path)


def upload_to_gcs(
    local_path: str,
    bucket_name: str,
    blob_name: str,
    content_type: str = "application/octet-stream",
) -> str:
    """Upload the file and return the gs:// URL."""
    bucket = _gcs().bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path, content_type=content_type)
    return f"gs://{bucket_name}/{blob_name}"


def get_signed_url(gs_url: str, expiration_hours: int = 720) -> str:
    """Return a v4 signed URL valid for expiration_hours (default 30 days)."""
    import json
    from google.oauth2 import service_account

    bucket_name, blob_name = _parse_gs_url(gs_url)

    # Cloud Run usa Compute Engine credentials (token-only), que no pueden firmar.
    # Usamos la SA key de GCP_SA_KEY_JSON para obtener credenciales firmables.
    sa_key_json = os.environ.get("GCP_SA_KEY_JSON", "")
    if sa_key_json:
        signing_creds = service_account.Credentials.from_service_account_info(
            json.loads(sa_key_json)
        )
        client = storage.Client(credentials=signing_creds)
    else:
        signing_creds = None
        client = _gcs()

    blob = client.bucket(bucket_name).blob(blob_name)
    return blob.generate_signed_url(
        version="v4",
        expiration=timedelta(hours=expiration_hours),
        method="GET",
        credentials=signing_creds,
    )
