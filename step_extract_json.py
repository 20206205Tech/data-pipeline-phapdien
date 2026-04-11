import ast
import json
import os
import re

from loguru import logger

import env
from utils.config_by_path import ConfigByPath
from utils.google_drive import get_drive_service, sync_local_file_to_drive

config_by_path = ConfigByPath(__file__)

PATH_PARENT_EXTRACTED = os.path.join(
    env.PATH_FOLDER_DATA, "step_download_zip", "extracted"
)
FILE_JS_NAME = "jsonData.js"
REQUIRED_VARS = ["jdChuDe", "jdDeMuc", "jdAllTree"]
PATH_FOLDER_OUTPUT = config_by_path.PATH_FOLDER_OUTPUT


def find_file_recursive(base_path, target_name):
    for root, dirs, files in os.walk(base_path):
        if target_name in files:
            return os.path.join(root, target_name)
    return None


def process_js_to_json(file_path):
    found_data = {}
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        for var in REQUIRED_VARS:
            pattern = rf"(?:var|let|const)?\s*{var}\s*=\s*(\[.*?\])(?:\s*;|\s*\n|$)"
            match = re.search(pattern, content, re.DOTALL)
            if match:
                try:
                    found_data[var] = ast.literal_eval(match.group(1).strip())
                except Exception as e:
                    logger.error(f"❌ Lỗi parsing biến {var}: {e}")

        return found_data
    except Exception as e:
        logger.error(f"❌ Lỗi đọc file JS: {e}")
        return {}


def main():
    service = get_drive_service()
    drive_folder_id = config_by_path.GOOGLE_DRIVE_FOLDER_ID

    js_local_path = find_file_recursive(PATH_PARENT_EXTRACTED, FILE_JS_NAME)
    if not js_local_path:
        logger.error(f"❌ Không tìm thấy {FILE_JS_NAME}.")
        return

    # 1. Đồng bộ file JS gốc lên Drive (Sử dụng hàm gộp tự động check MD5)
    sync_local_file_to_drive(
        service, js_local_path, drive_folder_id, FILE_JS_NAME, "application/javascript"
    )

    logger.info("🛠️ Bắt đầu trích xuất các biến dữ liệu từ file JS...")
    extracted_dict = process_js_to_json(js_local_path)

    # 2. Xử lý từng biến và lưu thành các file JSON riêng biệt
    for var_name in REQUIRED_VARS:
        data = extracted_dict.get(var_name, [])
        if not data:
            continue

        file_json_name = f"{var_name}.json"
        local_json_path = os.path.join(PATH_FOLDER_OUTPUT, file_json_name)

        # Lưu ra file local
        os.makedirs(os.path.dirname(local_json_path), exist_ok=True)
        with open(local_json_path, "w", encoding="utf-8") as jf:
            json.dump(data, jf, ensure_ascii=False, indent=4)

        # 3. Đồng bộ các file JSON vừa bóc tách lên Drive (Tự động check MD5)
        sync_local_file_to_drive(
            service,
            local_json_path,
            drive_folder_id,
            file_json_name,
            "application/json",
        )


if __name__ == "__main__":
    main()
