import json
import os

import pandas as pd
from bs4 import BeautifulSoup
from loguru import logger

import env
from utils.config_by_path import ConfigByPath
from utils.google_drive import get_drive_service, sync_local_file_to_drive

config_by_path = ConfigByPath(__file__)

PATH_INPUT_HTML = os.path.join(
    env.PATH_FOLDER_DATA, "step_download_zip", "extracted", "demuc"
)

# Đường dẫn tới 3 file JSON
PATH_TREE_JSON = os.path.join(
    env.PATH_FOLDER_DATA, "step_extract_json", "jdAllTree.json"
)
PATH_CHUDE_JSON = os.path.join(
    env.PATH_FOLDER_DATA, "step_extract_json", "jdChuDe.json"
)
PATH_DEMUC_JSON = os.path.join(
    env.PATH_FOLDER_DATA, "step_extract_json", "jdDeMuc.json"
)


def load_metadata_dicts():
    """Đọc file Chủ đề và Đề mục thành Dictionary để Map tên nhanh chóng"""
    chude_dict = {}
    demuc_dict = {}
    try:
        if os.path.exists(PATH_CHUDE_JSON):
            with open(PATH_CHUDE_JSON, "r", encoding="utf-8") as f:
                for item in json.load(f):
                    chude_dict[str(item["Value"])] = str(item["Text"])

        if os.path.exists(PATH_DEMUC_JSON):
            with open(PATH_DEMUC_JSON, "r", encoding="utf-8") as f:
                for item in json.load(f):
                    demuc_dict[str(item["Value"])] = str(item["Text"])
    except Exception as e:
        logger.error(f"❌ Lỗi khi đọc metadata dicts: {e}")

    return chude_dict, demuc_dict


def parse_html_to_data(html_path, df_tree, chude_dict, demuc_dict):
    if not os.path.exists(html_path):
        return []
    with open(html_path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), "html.parser")

    # Chú ý: Key trong JSON là MAPC (viết hoa)
    list_ids = df_tree["MAPC"].tolist()
    final_data = []

    for i, current_id in enumerate(list_ids):
        start_node = soup.find("a", attrs={"name": current_id})
        if not start_node:
            continue

        next_id = list_ids[i + 1] if i + 1 < len(list_ids) else None
        content_buffer = []

        for sibling in start_node.find_all_next():
            if next_id and sibling.name == "a" and sibling.get("name") == next_id:
                break
            target_classes = ["pNoiDung", "pGhiChu", "pChiDan", "pChuong", "pDieu"]
            if sibling.name in ["p", "div"] and any(
                cls in sibling.get("class", []) for cls in target_classes
            ):
                if not any(parent in content_buffer for parent in sibling.parents):
                    txt = sibling.get_text(" ", strip=True)
                    if txt and txt not in content_buffer:
                        content_buffer.append(txt)

        row = df_tree.iloc[i]
        chu_de_id = str(row.get("ChuDeID", ""))
        de_muc_id_val = str(row.get("DeMucID", ""))

        final_data.append(
            {
                "id": str(row["ID"]),
                "chi_muc": str(row.get("ChiMuc", "")),
                "ten": str(row.get("TEN", "")),
                "mapc": str(row["MAPC"]),
                "chu_de_id": chu_de_id,
                "ten_chu_de": chude_dict.get(chu_de_id, ""),  # Lấy Tên Chủ Đề từ Dict
                "de_muc_id": de_muc_id_val,
                "ten_de_muc": demuc_dict.get(
                    de_muc_id_val, ""
                ),  # Lấy Tên Đề Mục từ Dict
                "content_html_clean": "\n".join(content_buffer),
            }
        )
    return final_data


def load_tree_dataframe():
    """Đọc toàn bộ file jdAllTree.json vào Pandas DataFrame"""
    if not os.path.exists(PATH_TREE_JSON):
        logger.error(f"❌ Không tìm thấy file cây mục lục tại: {PATH_TREE_JSON}")
        return None

    try:
        with open(PATH_TREE_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)

        df = pd.DataFrame(data)
        # Đảm bảo cột MAPC là chuỗi
        if "MAPC" in df.columns:
            df["MAPC"] = df["MAPC"].astype(str)
        return df
    except Exception as e:
        logger.error(f"❌ Lỗi khi đọc jdAllTree.json: {e}")
        return None


def main():
    service = get_drive_service()
    drive_folder_id = config_by_path.GOOGLE_DRIVE_FOLDER_ID

    if not os.path.exists(PATH_INPUT_HTML):
        logger.error(f"❌ Thư mục nguồn không tồn tại: {PATH_INPUT_HTML}")
        return

    logger.info("🌳 Đang tải dữ liệu cây mục lục (jdAllTree.json) vào bộ nhớ...")
    df_all_tree = load_tree_dataframe()
    if df_all_tree is None or df_all_tree.empty:
        return

    logger.info("📖 Đang tải Metadata (Chủ đề, Đề mục) vào bộ nhớ...")
    chude_dict, demuc_dict = load_metadata_dicts()

    html_files = [f for f in os.listdir(PATH_INPUT_HTML) if f.endswith(".html")]

    if env.ENVIRONMENT == "development":
        LIMIT = 50
        logger.warning(f"🛠️ Chế độ Development: Chỉ xử lý tối đa {LIMIT} file.")
        html_files = html_files[:LIMIT]

    logger.info(f"🚀 Bắt đầu xử lý {len(html_files)} file HTML...")

    for file_name in html_files:
        de_muc_id = file_name.replace(".html", "")
        local_path = os.path.join(PATH_INPUT_HTML, file_name)
        output_json_path = os.path.join(
            config_by_path.PATH_FOLDER_OUTPUT, f"{de_muc_id}.json"
        )

        # Lọc theo DeMucID (Key trong JSON)
        df_tree_demuc = df_all_tree[df_all_tree["DeMucID"] == de_muc_id].copy()

        if df_tree_demuc.empty:
            # Chỉ đếm số dòng khi xảy ra lỗi để tối ưu tốc độ
            with open(local_path, "r", encoding="utf-8") as f:
                line_count = sum(1 for _ in f)

            logger.warning(
                f"⚠️ Không tìm thấy dữ liệu cây cho DeMuc: {de_muc_id} (File HTML có {line_count} dòng)"
            )
            continue
        df_tree_demuc = df_tree_demuc.sort_values(by="MAPC", ascending=True)

        is_changed, _ = sync_local_file_to_drive(
            service, local_path, drive_folder_id, file_name, "text/html"
        )

        if not is_changed and os.path.exists(output_json_path):
            logger.info(f"⏭️ File HTML '{file_name}' không đổi. Bỏ qua bước Chunking.")
            continue

        logger.info(f"📦 Đang parse dữ liệu: {file_name}")
        extracted_data = parse_html_to_data(
            local_path, df_tree_demuc, chude_dict, demuc_dict
        )

        os.makedirs(os.path.dirname(output_json_path), exist_ok=True)
        with open(output_json_path, "w", encoding="utf-8") as f:
            json.dump(extracted_data, f, ensure_ascii=False, indent=4)

        logger.success(f"✅ Đã xử lý bóc tách JSON xong: {de_muc_id}.json")


if __name__ == "__main__":
    main()
