from langchain_postgres import PGVector
from sqlalchemy import create_engine

import env

from .embedding import embeddings_model

COLLECTION_NAME = "phap_dien_vectors"
CONNECTION_STRING = env.PHAP_DIEN_VECTOR_DATABASE

# 1. Khởi tạo Engine với cấu hình chống rớt kết nối (Keep-Alive)
engine = create_engine(
    CONNECTION_STRING,
    pool_pre_ping=True,  # QUAN TRỌNG: Ping kiểm tra kết nối trước khi thực thi, nếu rớt sẽ tự kết nối lại
    pool_recycle=60,  # Tự động làm mới kết nối sau mỗi 60 giây
)

# 2. Truyền engine thay vì chuỗi CONNECTION_STRING
vector_store = PGVector(
    embeddings=embeddings_model,
    collection_name=COLLECTION_NAME,
    connection=engine,
    use_jsonb=True,
)
