import json
import os

import dlt
from loguru import logger

import env
from utils.config_by_path import ConfigByPath
from utils.google_drive import get_drive_service, sync_local_file_to_drive

config_by_path = ConfigByPath(__file__)

# Thư mục chứa các file JSON được tạo ra từ bước trước
PATH_INPUT_JSON = os.path.join(env.PATH_FOLDER_DATA, "step_extract_json")

# Mapping tên file JSON với tên bảng sẽ tạo trong Postgres
TABLE_MAPPING = {
    "jdChuDe.json": "chu_de",
    "jdDeMuc.json": "de_muc",
    "jdAllTree.json": "all_tree",
}


def main():
    logger.info("🚀 Khởi tạo tiến trình Load JSON...")

    service = get_drive_service()
    drive_folder_id = config_by_path.GOOGLE_DRIVE_FOLDER_ID

    # Khởi tạo pipeline dlt
    pipeline = dlt.pipeline(
        pipeline_name="phapdien_pipeline",
        destination="postgres",
        dataset_name="public",
    )

    for file_name, table_name in TABLE_MAPPING.items():
        file_path = os.path.join(PATH_INPUT_JSON, file_name)

        if not os.path.exists(file_path):
            logger.warning(f"⚠️ Không tìm thấy file {file_name}. Bỏ qua.")
            continue

        # 1. Kiểm tra thay đổi & Đồng bộ lên Google Drive
        logger.info(f"🔍 Kiểm tra thay đổi file: {file_name}")
        is_changed, _ = sync_local_file_to_drive(
            service, file_path, drive_folder_id, file_name, "application/json"
        )

        # Nếu không có thay đổi (MD5 khớp), bỏ qua việc nạp vào database
        if not is_changed:
            logger.info(f"⏭️ File '{file_name}' không thay đổi. Bỏ qua nạp Database.")
            continue

        # 2. Nếu có thay đổi -> Nạp vào Database Postgres
        logger.info(f"📖 Đang đọc dữ liệu từ {file_name} để nạp vào DB...")
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not data:
            logger.warning(f"⚠️ File {file_name} rỗng.")
            continue

        logger.info(f"⏳ Đang nạp {len(data)} bản ghi vào bảng '{table_name}'...")

        # Chạy pipeline để đẩy dữ liệu
        # write_disposition="replace" sẽ xóa bảng cũ và tạo lại.
        # Rất phù hợp với dữ liệu tĩnh như pháp điển tải về theo đợt.
        load_info = pipeline.run(
            data, table_name=table_name, write_disposition="replace"
        )

        logger.success(f"✅ Đã nạp thành công vào bảng '{table_name}'.")
        logger.debug(load_info)


if __name__ == "__main__":
    main()
