import os
import shutil
import zipfile
from datetime import datetime

import requests
from loguru import logger

import env
from utils.config_by_path import ConfigByPath
from utils.google_drive import (
    get_drive_service,
    sync_local_file_to_drive,
    upload_file_to_drive_with_metadata,
)

config_by_path = ConfigByPath(__file__)

# Dùng một tên cố định để dễ dàng kiểm tra đồng bộ chính xác
FILE_NAME_LATEST = "phapdien_latest.zip"
PATH_ZIP_LOCAL = os.path.join(config_by_path.PATH_FOLDER_OUTPUT, FILE_NAME_LATEST)
PATH_EXTRACT_DIR = os.path.join(config_by_path.PATH_FOLDER_OUTPUT, "extracted")


def download_file(url, dest_path):
    logger.info(f"🚀 Đang tải file từ: {url}")
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    with open(dest_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    logger.success(f"Tải xong: {dest_path}")


def extract_zip(zip_path, extract_to):
    if os.path.exists(extract_to):
        shutil.rmtree(extract_to)
    os.makedirs(extract_to, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)
    logger.success(f"📦 Đã giải nén vào: {extract_to}")


def main():
    # 1. Tải file về luôn (đảm bảo hàm sync có file thực tế để kiểm tra MD5)
    download_file(env.URL_SOURCE, PATH_ZIP_LOCAL)

    service = get_drive_service()
    drive_folder_id = config_by_path.GOOGLE_DRIVE_FOLDER_ID

    # 2. Kiểm tra đồng bộ cực chuẩn xác bằng hàm sync_local_file_to_drive
    is_changed, _ = sync_local_file_to_drive(
        service, PATH_ZIP_LOCAL, drive_folder_id, FILE_NAME_LATEST, "application/zip"
    )

    # 3. --- GHI TRẠNG THÁI CHO GITHUB ACTIONS ---
    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"changed={'true' if is_changed else 'false'}\n")

    # 4. --- LOGIC CHỈ CHẠY NẾU FILE CÓ THAY ĐỔI ---
    if is_changed:
        logger.info("⚡ File ZIP có sự thay đổi. Đang tiến hành giải nén dữ liệu...")
        extract_zip(PATH_ZIP_LOCAL, PATH_EXTRACT_DIR)

        # Lưu thêm một bản backup có gắn datetime lên Google Drive (Nếu bạn cần)
        now_str = datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")
        history_file_name = f"phapdien_{now_str}.zip"
        history_path = os.path.join(
            config_by_path.PATH_FOLDER_OUTPUT, history_file_name
        )

        logger.info(f"💾 Đang tạo bản lưu trữ: {history_file_name}")
        shutil.copy(PATH_ZIP_LOCAL, history_path)
        upload_file_to_drive_with_metadata(
            service,
            history_path,
            drive_folder_id,
            history_file_name,
            mimetype="application/zip",
        )
        os.remove(history_path)  # Dọn dẹp bản copy

    else:
        logger.success(
            "✅ File ZIP không đổi. Bỏ qua bước giải nén và các step tiếp theo trên GitHub Actions."
        )


if __name__ == "__main__":
    main()
