import json
import os

from langchain_core.documents import Document
from loguru import logger

import env
from rag.vectorstore import vector_store
from utils.config_by_path import ConfigByPath
from utils.google_drive import get_drive_service, sync_local_file_to_drive

config_by_path = ConfigByPath(__file__)

PATH_INPUT_JSON = os.path.join(env.PATH_FOLDER_DATA, "step_rag_chunking")


def main():
    service = get_drive_service()
    drive_folder_id = config_by_path.GOOGLE_DRIVE_FOLDER_ID

    json_files = [f for f in os.listdir(PATH_INPUT_JSON) if f.endswith(".json")]
    if env.ENVIRONMENT == "development":
        LIMIT = 5
        logger.warning(f"🛠️ Chế độ Development: Chỉ xử lý tối đa {LIMIT} file.")
        json_files = json_files[:LIMIT]

    for file_name in json_files:
        local_path = os.path.join(PATH_INPUT_JSON, file_name)

        is_changed, _ = sync_local_file_to_drive(
            service, local_path, drive_folder_id, file_name, "application/json"
        )

        # if not is_changed:
        #     continue

        logger.info(f"🚀 Đang nạp mới Vector cho: {file_name}")

        with open(local_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        documents = []
        doc_ids = []
        for item in data:
            if item.get("content_html_clean"):
                # Bơm Tên Chủ Đề và Tên Đề Mục vào đầu văn bản để làm giàu ngữ cảnh RAG
                rich_content = (
                    f"Chủ đề: {item.get('ten_chu_de', 'Không rõ')}\n"
                    f"Đề mục: {item.get('ten_de_muc', 'Không rõ')}\n"
                    f"Nội dung {item.get('ten', '')}:\n{item['content_html_clean']}"
                )

                doc = Document(
                    page_content=rich_content,
                    metadata={**item, "source_file": file_name},
                )
                documents.append(doc)
                doc_ids.append(str(item.get("id")))

        if documents:
            vector_store.add_documents(documents=documents, ids=doc_ids)
            logger.success(f"✅ Đã nạp Vector thành công: {file_name}")


if __name__ == "__main__":
    main()
