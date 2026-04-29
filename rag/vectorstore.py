from langchain_qdrant import QdrantVectorStore
from loguru import logger
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams

import env

from .embedding import embeddings_model

# 1. Khởi tạo Qdrant Client với timeout cao hơn (60 giây)
client = QdrantClient(
    url=env.QDRANT_URL,
    api_key=env.QDRANT_API_KEY,
    timeout=60.0,  # <--- THÊM DÒNG NÀY
)

collection_name = env.QDRANT_COLLECTION_NAME

# 2. Kiểm tra và tự động tạo Collection nếu chưa tồn tại
try:
    if not client.collection_exists(collection_name=collection_name):
        logger.info(f"Đang tạo mới Qdrant collection: {collection_name}")
        # keepitreal/vietnamese-sbert output vector size = 768
        client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=768, distance=Distance.COSINE),
        )
        logger.info("✅ Tạo Qdrant collection thành công!")
    else:
        logger.info(f"✅ Qdrant collection '{collection_name}' đã sẵn sàng.")
except Exception as e:
    logger.error(f"❌ Lỗi khi kiểm tra/tạo Qdrant collection: {e}")

# 3. Khởi tạo Vector Store
vector_store = QdrantVectorStore(
    client=client,
    collection_name=collection_name,
    embedding=embeddings_model,
)
