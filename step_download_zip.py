import os
import shutil
import zipfile

import requests
from loguru import logger
from tqdm import tqdm

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
    total_size = int(response.headers.get("content-length", 0))
    with open(dest_path, "wb") as f, tqdm(
        desc=f"Downloading {os.path.basename(dest_path)}",
        total=total_size,
        unit="iB",
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                size = f.write(chunk)
                bar.update(size)
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

    # GỌI HÀM "SUPER FUNCTION" Ở ĐÂY
    is_changed, _ = sync_local_file_to_drive(
        service, PATH_ZIP_LOCAL, drive_folder_id, FILE_NAME, "application/zip"
    )

    # Nếu file ZIP có thay đổi HOẶC chưa từng giải nén -> Tiến hành giải nén
    if is_changed or not os.path.exists(PATH_EXTRACT_DIR):
        logger.info("⚡ Đang chuẩn bị dữ liệu local (giải nén)...")
        extract_zip(PATH_ZIP_LOCAL, PATH_EXTRACT_DIR)
    else:
        logger.info("✅ Thư mục giải nén local đã sẵn sàng và file ZIP không đổi.")


if __name__ == "__main__":
    main()
