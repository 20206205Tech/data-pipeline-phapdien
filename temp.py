import torch
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_postgres import PGVector
from loguru import logger

import env

COLLECTION_NAME = "phap_dien_vectors"
CONNECTION_STRING = env.PHAP_DIEN_VECTOR_DATABASE


def get_device():
    if torch.cuda.is_available():
        return "cuda"
    if torch.backends.mps.is_available():
        return "mps"
    return "cpu"


# =========================================================================
# BƯỚC 1: KHỞI TẠO MÔ HÌNH VÀ DATABASE CHỈ 1 LẦN DUY NHẤT ĐỂ TỐI ƯU TỐC ĐỘ
# =========================================================================
logger.info("⏳ Đang tải mô hình Embedding vào bộ nhớ (chỉ mất vài giây)...")
embeddings = HuggingFaceEmbeddings(
    model_name="keepitreal/vietnamese-sbert",
    model_kwargs={"device": get_device()},
    encode_kwargs={"normalize_embeddings": True},
)

logger.info("🔌 Đang kết nối tới Vector Database...")
vector_store = PGVector(
    embeddings=embeddings,
    collection_name=COLLECTION_NAME,
    connection=CONNECTION_STRING,
    use_jsonb=True,
)


def test_search(query_text):
    logger.info(f"🔍 Đang tìm kiếm: '{query_text}'")

    # similarity_search_with_score trả về list các tuple (Document, Score)
    # Với Langchain PGVector mặc định (Cosine Distance): Càng gần 0 càng tốt
    results = vector_store.similarity_search_with_score(query_text, k=3)

    if not results:
        logger.warning("⚠️ Không tìm thấy nội dung nào tương đồng.")
        return

    print("\n" + "=" * 60)
    print(f"🎯 KẾT QUẢ TÌM KIẾM CHO: '{query_text}'")
    print("=" * 60)

    for i, (doc, score) in enumerate(results, 1):
        print(f"\n[{i}] Độ khoảng cách (Score): {score:.4f} (Càng nhỏ càng khớp)")

        # In ra các Metadata xịn mà ta đã lấy từ json
        print(f"🏷️ Chủ đề: {doc.metadata.get('ten_chu_de', 'Không rõ')}")
        print(
            f"📂 Đề mục: {doc.metadata.get('ten_de_muc', 'Không rõ')} (ID: {doc.metadata.get('de_muc_id')})"
        )
        print(f"📍 Mã PC: {doc.metadata.get('mapc')}")

        print(f"📄 Nội dung trích đoạn:\n{doc.page_content[:300]}...\n")
        print("-" * 40)


if __name__ == "__main__":
    # Test thử một vài câu hỏi về pháp luật
    user_queries = [
        "Quy định về thời gian làm việc của người lao động",
        "Thủ tục đăng ký khai sinh cho trẻ em",
        "Mức phạt vi phạm nồng độ cồn khi lái xe",
    ]

    for q in user_queries:
        test_search(q)
        print("\n")
