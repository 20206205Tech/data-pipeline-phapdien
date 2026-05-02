import os
import shutil
import zipfile

import requests
from loguru import logger

import env
from utils.config_by_path import ConfigByPath
from utils.google_drive import get_drive_service, sync_local_file_to_drive

config_by_path = ConfigByPath(__file__)

FILE_NAME = "phapdien_latest.zip"
PATH_ZIP_LOCAL = os.path.join(config_by_path.PATH_FOLDER_OUTPUT, FILE_NAME)
PATH_EXTRACT_DIR = os.path.join(config_by_path.PATH_FOLDER_OUTPUT, "extracted")


def download_file(url, dest_path):
    logger.info(f"🚀 Đang tải file từ: {url}")
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    # Ghi file theo từng chunk nhỏ để tối ưu bộ nhớ
    with open(dest_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    logger.success(f"✅ Tải xong: {dest_path}")


def extract_zip(zip_path, extract_to):
    if os.path.exists(extract_to):
        shutil.rmtree(extract_to)
    os.makedirs(extract_to, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)
    logger.success(f"📦 Đã giải nén vào: {extract_to}")


def main():
    download_file(env.URL_SOURCE, PATH_ZIP_LOCAL)

    service = get_drive_service()
    drive_folder_id = config_by_path.GOOGLE_DRIVE_FOLDER_ID

    # Kiểm tra đồng bộ file với Google Drive
    is_changed, _ = sync_local_file_to_drive(
        service, PATH_ZIP_LOCAL, drive_folder_id, FILE_NAME, "application/zip"
    )

    # --- GHI TRẠNG THÁI CHO GITHUB ACTIONS ---
    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"changed={'true' if is_changed else 'false'}\n")

    # --- LOGIC GIẢI NÉN CÓ ĐIỀU KIỆN ---
    if is_changed:
        logger.info("⚡ File ZIP có sự thay đổi. Đang tiến hành giải nén dữ liệu...")
        extract_zip(PATH_ZIP_LOCAL, PATH_EXTRACT_DIR)
    else:
        logger.info(
            "✅ File ZIP không đổi. Bỏ qua bước giải nén và các step tiếp theo trên GitHub Actions."
        )


if __name__ == "__main__":
    main()
