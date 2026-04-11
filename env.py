import os

from environs import Env
from loguru import logger

env = Env()
logger.info("Loading environment variables...")


PATH_FILE_ENV = os.path.abspath(__file__)
PATH_FOLDER_PROJECT = os.path.dirname(PATH_FILE_ENV)
PATH_FOLDER_DATA = os.path.join(PATH_FOLDER_PROJECT, "data")
PATH_FOLDER_DOCS = os.path.join(PATH_FOLDER_PROJECT, "docs")


if not os.path.exists(PATH_FOLDER_DATA):
    os.makedirs(PATH_FOLDER_DATA)

if not os.path.exists(PATH_FOLDER_DOCS):
    os.makedirs(PATH_FOLDER_DOCS)


os.environ["LANGCHAIN_TRACING_V2"] = "false"
os.environ["LANGSMITH_TRACING"] = "false"


ENVIRONMENT = env.str("ENVIRONMENT", "production")


GOOGLE_DRIVE_TOKEN = env.str("GOOGLE_DRIVE_TOKEN")
PHAP_DIEN_GOOGLE_DRIVE_FOLDER_ID = env.str("PHAP_DIEN_GOOGLE_DRIVE_FOLDER_ID")


DATA_PIPELINE_PHAP_DIEN_DATABASE_URL = env.str("DATA_PIPELINE_PHAP_DIEN_DATABASE_URL")


# if ENVIRONMENT == "development":
#     DATA_PIPELINE_PHAP_DIEN_DATABASE_URL = (
#         "postgresql://postgres:postgres@localhost:5432/postgres"
#     )


PHAP_DIEN_VECTOR_DATABASE = DATA_PIPELINE_PHAP_DIEN_DATABASE_URL
DESTINATION__POSTGRES__CREDENTIALS = DATA_PIPELINE_PHAP_DIEN_DATABASE_URL.replace(
    "-pooler", ""
)
os.environ["DESTINATION__POSTGRES__CREDENTIALS"] = DESTINATION__POSTGRES__CREDENTIALS


URL_SOURCE = "https://phapdien.moj.gov.vn/TraCuuPhapDien/Files/BoPhapDienDienTu.zip"
# if ENVIRONMENT == "development":
#     URL_SOURCE = (
#         "https://github.com/20206205Tech/infra-by-terraform/archive/refs/heads/main.zip"
#     )
