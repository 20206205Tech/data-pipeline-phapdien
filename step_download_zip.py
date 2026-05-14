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
    get_latest_file_info_by_prefix,
    upload_file_to_drive_with_metadata,
)

config_by_path = ConfigByPath(__file__)

FILE_PREFIX = "phapdien_"
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

    logger.success(f"Tải xong: {dest_path}")


def extract_zip(zip_path, extract_to):
    if os.path.exists(extract_to):
        shutil.rmtree(extract_to)
    os.makedirs(extract_to, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)
    logger.success(f"📦 Đã giải nén vào: {extract_to}")


def get_source_metadata(url):
    """Lấy metadata từ URL nguồn (ETag, Content-Length)"""
    try:
        response = requests.head(url, allow_redirects=True, timeout=30)
        response.raise_for_status()
        etag = response.headers.get("ETag", "").strip('"')
        content_length = response.headers.get("Content-Length", "")
        return {"etag": etag, "size": content_length}
    except Exception as e:
        logger.warning(f"Không thể lấy metadata từ URL: {e}")
        return None


def main():
    service = get_drive_service()
    drive_folder_id = config_by_path.GOOGLE_DRIVE_FOLDER_ID

    # 1. Lấy metadata từ nguồn
    source_metadata = get_source_metadata(env.URL_SOURCE)
    source_tag = (
        f"etag:{source_metadata['etag']}|size:{source_metadata['size']}"
        if source_metadata
        else None
    )

    # 2. Tìm file mới nhất trên Drive
    latest_drive_file = get_latest_file_info_by_prefix(
        service, drive_folder_id, FILE_PREFIX
    )

    is_changed = True
    if latest_drive_file and source_tag:
        drive_description = latest_drive_file.get("description", "")
        if source_tag == drive_description:
            logger.success(
                f"✅ File trên Drive đã là mới nhất ({latest_drive_file['name']}). Bỏ qua tải xuống."
            )
            is_changed = False

    # 3. Xử lý tải xuống và upload nếu có thay đổi
    now_str = datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")
    current_file_name = f"{FILE_PREFIX}{now_str}.zip"
    path_zip_local = os.path.join(config_by_path.PATH_FOLDER_OUTPUT, current_file_name)

    if is_changed:
        download_file(env.URL_SOURCE, path_zip_local)

        # Upload lên Drive với metadata mới
        upload_file_to_drive_with_metadata(
            service,
            path_zip_local,
            drive_folder_id,
            current_file_name,
            mimetype="application/zip",
            description=source_tag,
        )

        logger.info("⚡ Đang tiến hành giải nén dữ liệu...")
        extract_zip(path_zip_local, PATH_EXTRACT_DIR)

        # Dọn dẹp file local sau khi đã upload và giải nén
        if os.path.exists(path_zip_local):
            os.remove(path_zip_local)
    else:
        logger.info("File không đổi. Bỏ qua các bước tiếp theo.")

    # --- GHI TRẠNG THÁI CHO GITHUB ACTIONS ---
    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"changed={'true' if is_changed else 'false'}\n")


if __name__ == "__main__":
    main()
