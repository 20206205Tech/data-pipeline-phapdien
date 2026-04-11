import io
import json
import mimetypes

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from loguru import logger

import env
from utils.hash_helper import calculate_file_md5

SCOPES = ["https://www.googleapis.com/auth/drive.file"]


# =====================================================================
# 1. CORE AUTHENTICATION & FOLDER MANAGEMENT
# =====================================================================


def get_drive_service():
    """Khởi tạo và xác thực API Google Drive"""
    creds = None
    creds_info = json.loads(env.GOOGLE_DRIVE_TOKEN)
    creds = Credentials.from_authorized_user_info(creds_info, SCOPES)

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except Exception as e:
            logger.error(f"Không thể refresh token: {e}")
            creds = None

    if not creds:
        raise Exception(
            "Không tìm thấy thông tin xác thực! Hãy chạy lấy token tại local trước."
        )

    return build("drive", "v3", credentials=creds)


def get_or_create_drive_folder(service, folder_name, parent_id=None):
    """Tìm ID của thư mục, nếu chưa có thì tạo mới"""
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    if parent_id:
        query += f" and '{parent_id}' in parents"

    response = (
        service.files()
        .list(q=query, spaces="drive", fields="files(id, name)")
        .execute()
    )
    files = response.get("files", [])

    if files:
        folder_id = files[0]["id"]
        logger.info(f"Folder '{folder_name}' đã tồn tại | ID: {folder_id}")
        return folder_id
    else:
        file_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
        }
        if parent_id:
            file_metadata["parents"] = [parent_id]

        file = service.files().create(body=file_metadata, fields="id").execute()
        folder_id = file.get("id")
        logger.info(f"Đã tạo folder mới '{folder_name}' | ID: {folder_id}")
        return folder_id


# =====================================================================
# 2. GET FILE INFORMATION & DOWNLOAD
# =====================================================================


def get_drive_url(drive_id, is_folder=False):
    """Tạo link xem trực tiếp trên Google Drive từ file ID"""
    if is_folder:
        return f"https://drive.google.com/drive/folders/{drive_id}"
    return f"https://drive.google.com/file/d/{drive_id}/view"


def download_from_drive(service, file_id):
    """Tải nội dung file từ Google Drive về bộ nhớ (bytes)"""
    file_url = get_drive_url(file_id)
    logger.debug(f"Đang tải file từ URL: {file_url}")

    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()

    return fh.getvalue()


def get_drive_file_info(service, folder_id, file_name):
    """Lấy thông tin file (id, md5Checksum) dựa vào tên và thư mục chứa"""
    query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
    try:
        results = (
            service.files().list(q=query, fields="files(id, md5Checksum)").execute()
        )
        files = results.get("files", [])
        return files[0] if files else None
    except Exception as e:
        logger.error(f"Lỗi khi lấy thông tin file {file_name}: {e}")
        return None


def get_drive_hashes_in_folder(service, folder_id):
    """Lấy danh sách mã MD5 của TẤT CẢ các file trong một folder (Dùng cho Chunking)"""
    try:
        results = (
            service.files()
            .list(
                q=f"'{folder_id}' in parents and trashed = false",
                fields="files(name, md5Checksum)",
                pageSize=1000,  # Lấy tối đa 1000 file mỗi request để tránh sót
            )
            .execute()
        )
        return {f["name"]: f.get("md5Checksum") for f in results.get("files", [])}
    except Exception as e:
        logger.error(f"Lỗi khi lấy danh sách hash từ folder {folder_id}: {e}")
        return {}


# =====================================================================
# 3. UPLOAD & SYNC LOGIC (THE "SUPER FUNCTIONS")
# =====================================================================


def upsert_file_to_drive(service, local_path, folder_id, file_name, mimetype=None):
    """Cập nhật (Ghi đè) nếu file đã tồn tại, Tạo mới nếu chưa có"""
    if mimetype is None:
        mimetype, _ = mimetypes.guess_type(local_path)
        if mimetype is None:
            mimetype = "application/octet-stream"

    existing_file = get_drive_file_info(service, folder_id, file_name)
    media = MediaFileUpload(local_path, mimetype=mimetype, resumable=True)

    try:
        if existing_file:
            file_id = existing_file["id"]
            logger.debug(f"🔄 Đang ghi đè file '{file_name}' (ID: {file_id})")
            service.files().update(fileId=file_id, media_body=media).execute()
            return file_id
        else:
            logger.debug(f"🆕 Đang tạo file mới '{file_name}'")
            file_metadata = {"name": file_name, "parents": [folder_id]}
            file = (
                service.files()
                .create(body=file_metadata, media_body=media, fields="id")
                .execute()
            )
            return file.get("id")
    except Exception as e:
        logger.error(f"❌ Lỗi khi upsert file {file_name}: {e}")
        return None


def sync_local_file_to_drive(service, local_path, folder_id, file_name, mimetype=None):
    """
    Kiểm tra Hash và đồng bộ file lên Google Drive một cách thông minh.
    Trả về tuple: (is_changed: bool, drive_file_id: str)
    """
    # 1. Tính Hash local
    local_md5 = calculate_file_md5(local_path)
    if not local_md5:
        logger.error(f"Không thể tính MD5 cho file local: {local_path}")
        return False, None

    # 2. Kiểm tra thông tin trên Drive
    drive_file_info = get_drive_file_info(service, folder_id, file_name)

    # 3. So sánh Hash
    if drive_file_info and drive_file_info.get("md5Checksum") == local_md5:
        logger.info(f"⏭️ File '{file_name}' không thay đổi (MD5 khớp). Bỏ qua upload.")
        return False, drive_file_info.get("id")

    # 4. Upload/Ghi đè nếu có sự khác biệt
    action = "Ghi đè file cũ" if drive_file_info else "Tạo file mới"
    logger.info(
        f"🚀 Phát hiện thay đổi ({action}). Đang đồng bộ '{file_name}' lên Drive..."
    )

    drive_id = upsert_file_to_drive(service, local_path, folder_id, file_name, mimetype)

    if drive_id:
        logger.success(f"✅ Đồng bộ thành công: {file_name}")
        # BỔ SUNG LOGIC HIỂN THỊ LINK DEBUG Ở ĐÂY
        logger.debug(f"Link: {get_drive_url(drive_id)}")

    return True, drive_id
