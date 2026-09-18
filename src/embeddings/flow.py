import json
import os
from functools import cache
from urllib.parse import urlparse

import onnxruntime as ort
from fastembed import SparseTextEmbedding, TextEmbedding
from prefect import flow, task
from psycopg import DataError

from embeddings.storage import upsert_embeddings
from upload_events import s3_client

DEFAULT_DENSE_MODEL = "BAAI/bge-small-en-v1.5"
DEFAULT_SPARSE_MODEL = "Qdrant/bm42-all-minilm-l6-v2-attentions"
PGVECTOR_MAX_SPARSE_DIMENSIONS = 1_000_000_000


def available_embedding_providers(device: str = "auto") -> tuple[str, ...]:
    """Select CUDA when requested and available, with a CPU fallback."""
    available = set(ort.get_available_providers())
    if device not in {"auto", "cpu", "cuda"}:
        raise ValueError("embedding device must be auto, cpu, or cuda")
    if device == "cuda" and "CUDAExecutionProvider" not in available:
        raise RuntimeError("CUDA was requested but its ONNX provider is unavailable")
    if device != "cpu" and "CUDAExecutionProvider" in available:
        return ("CUDAExecutionProvider", "CPUExecutionProvider")
    return ("CPUExecutionProvider",)


@cache
def _dense_model(model_name: str, providers: tuple[str, ...]):
    return TextEmbedding(model_name=model_name, providers=list(providers))


@cache
def _sparse_model(model_name: str, providers: tuple[str, ...]):
    return SparseTextEmbedding(model_name=model_name, providers=list(providers))


def _sparse_dimension(model_name: str) -> int:
    for model in SparseTextEmbedding.list_supported_models():
        if model["model"].lower() == model_name.lower():
            # BM25/BM42 return 31-bit MurmurHash IDs rather than vocabulary IDs.
            # pgvector supports sparse vectors with at most one billion dimensions.
            if model.get("requires_idf"):
                return PGVECTOR_MAX_SPARSE_DIMENSIONS
            dimension = model.get("vocab_size")
            if dimension:
                return int(dimension)
            break
    raise ValueError(f"cannot determine sparse vector dimension for {model_name}")


def _normalise_sparse_vector(vector, dimension: int) -> dict:
    """Fit FastEmbed indices into pgvector and merge rare hash collisions."""
    values_by_index: dict[int, float] = {}
    for raw_index, raw_value in zip(vector.indices, vector.values, strict=True):
        index = int(raw_index) % dimension
        values_by_index[index] = values_by_index.get(index, 0.0) + float(raw_value)

    return {
        "indices": list(values_by_index),
        "values": list(values_by_index.values()),
        "dimension": dimension,
    }


def _read_s3_json(client, uri: str) -> dict:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise ValueError(f"invalid S3 URI: {uri}")
    response = client.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
    return json.loads(response["Body"].read())


@task(retries=2, persist_result=False)
def load_chunks(manifest_uri: str) -> list[dict]:
    """Load chunks through the immutable ingestion manifest."""
    client = s3_client()
    manifest = _read_s3_json(client, manifest_uri)
    artifact = next(
        (
            item
            for item in manifest.get("artifacts", [])
            if item.get("name") == "chunks.jsonl"
        ),
        None,
    )
    if artifact is None:
        raise ValueError("ingestion manifest has no chunks.jsonl artifact")

    parsed = urlparse(artifact["uri"])
    response = client.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
    lines = response["Body"].read().decode().splitlines()
    return [json.loads(line) for line in lines if line.strip()]


@task(retries=2, persist_result=False)
def generate_dense(
    chunks: list[dict], model_name: str, providers: tuple[str, ...]
) -> list[list[float]]:
    model = _dense_model(model_name, providers)
    return [
        vector.tolist() for vector in model.embed(chunk["text"] for chunk in chunks)
    ]


@task(retries=2, persist_result=False)
def generate_sparse(
    chunks: list[dict], model_name: str, providers: tuple[str, ...]
) -> list[dict]:
    model = _sparse_model(model_name, providers)
    dimension = _sparse_dimension(model_name)
    return [
        _normalise_sparse_vector(vector, dimension)
        for vector in model.embed(chunk["text"] for chunk in chunks)
    ]


def generate_query_embeddings(
    query: str,
    dense_model_name: str,
    sparse_model_name: str,
    providers: tuple[str, ...],
) -> tuple[list[float], dict]:
    """Generate query vectors with each model's query-specific path."""
    dense = next(_dense_model(dense_model_name, providers).query_embed(query))
    sparse = next(_sparse_model(sparse_model_name, providers).query_embed(query))
    return (
        dense.tolist(),
        _normalise_sparse_vector(sparse, _sparse_dimension(sparse_model_name)),
    )


def _retry_transient_store_error(_task, _task_run, state) -> bool:
    try:
        failure = state.result(raise_on_failure=False)
    except Exception:  # noqa: BLE001 - infrastructure errors should use the retry policy
        return True
    return not isinstance(failure, (DataError, ValueError))


@task(
    retries=3,
    retry_condition_fn=_retry_transient_store_error,
    persist_result=False,
)
def store_embeddings(
    document_id: str,
    ingestion_id: str,
    chunks: list[dict],
    dense_vectors: list[list[float]],
    sparse_vectors: list[dict],
    dense_model_name: str,
    sparse_model_name: str,
) -> None:
    upsert_embeddings(
        document_id=document_id,
        ingestion_id=ingestion_id,
        chunks=chunks,
        dense_vectors=dense_vectors,
        sparse_vectors=sparse_vectors,
        dense_model=dense_model_name,
        sparse_model=sparse_model_name,
    )


@flow(name="embed-ingestion")
def embed_ingestion(
    completion_event: dict,
    dense_model_name: str = DEFAULT_DENSE_MODEL,
    sparse_model_name: str = DEFAULT_SPARSE_MODEL,
    device: str = "auto",
) -> None:
    """Generate both vector types, then atomically store the complete document."""
    providers = available_embedding_providers(device)
    chunks = load_chunks.submit(completion_event["manifest_uri"])
    dense = generate_dense.submit(chunks, dense_model_name, providers)
    sparse = generate_sparse.submit(chunks, sparse_model_name, providers)
    stored = store_embeddings.submit(
        document_id=completion_event["document_id"],
        ingestion_id=completion_event["ingestion_id"],
        chunks=chunks,
        dense_vectors=dense,
        sparse_vectors=sparse,
        dense_model_name=dense_model_name,
        sparse_model_name=sparse_model_name,
    )
    stored.result()


def configured_models() -> tuple[str, str, str]:
    return (
        os.getenv("DENSE_EMBEDDING_MODEL", DEFAULT_DENSE_MODEL),
        os.getenv("SPARSE_EMBEDDING_MODEL", DEFAULT_SPARSE_MODEL),
        os.getenv("EMBEDDING_DEVICE", "auto").lower(),
    )
