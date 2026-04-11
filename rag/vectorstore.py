from langchain_postgres import PGVector

import env

from .embedding import embeddings_model

COLLECTION_NAME = "phap_dien_vectors"
CONNECTION_STRING = env.PHAP_DIEN_VECTOR_DATABASE

vector_store = PGVector(
    embeddings=embeddings_model,
    collection_name=COLLECTION_NAME,
    connection=CONNECTION_STRING,
    use_jsonb=True,
)
