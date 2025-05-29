import io
import time
import traceback
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from box_sdk_gen import BoxClient, BoxJWTAuth, JWTConfig, BoxAPIError
# from databricks.connect import DatabricksSession
from config import get_logger, SUPPORTED_EXTENSIONS, BOX_IMPERSONATE_USER_ID
from vector_store_service import VectorStoreService
# from databricks_service import DatabricksService
from keyvault_service import KeyVaultService

logger = get_logger("BoxService")

class BoxToVectorStore:
    def __init__(self, kv_service, openai_api_key, vector_store_id, full_load,
                 supported_extensions=SUPPORTED_EXTENSIONS, max_buffer_size=50, box_impersonate_user_id=BOX_IMPERSONATE_USER_ID):
        self.vector_store = VectorStoreService(openai_api_key, vector_store_id)
        # self.databricks_service = DatabricksService(databricks_store_table_name)
        self.supported_extensions = supported_extensions
        self.max_buffer_size = max_buffer_size
        self.box_user_id = box_impersonate_user_id
        self.full_load = full_load

        self.today_est = datetime.now(ZoneInfo("America/New_York"))
        self.cutoff_date = self.today_est.date() - timedelta(days=2) if self.today_est.weekday() == 5 else self.today_est.date() - timedelta(days=3)

        # self.spark = DatabricksSession.builder.getOrCreate()
        # self.df_current = self.spark.table(databricks_store_table_name)
        self.box_client = self._setup_box_client(kv_service)
        # self.vector_store.total_files_processed = 0
        # self.vector_store.successful_uploads = 0
        # self.vector_store.invalid_files = 0

    def _setup_box_client(self, kv_service: KeyVaultService) -> BoxClient:
        logger.info(f"📅 Processing documents modified from {self.cutoff_date} to {self.today_est.date()} (EST)")
        jwt_config = JWTConfig(
            client_id=kv_service.get_secret("Box-CLI-Automation-ClientID"),
            client_secret=kv_service.get_secret("Box-CLI-Automation-ClientSecret"),
            jwt_key_id=kv_service.get_secret("Box-CLI-Automation-PublicKeyID"),
            private_key=kv_service.get_secret("Box-CLI-Automation-PrivateKey").replace("\\n", "\n"),
            private_key_passphrase=kv_service.get_secret("Box-CLI-Automation-Passphrase"),
            user_id=self.box_user_id,
        )
        auth = BoxJWTAuth(config=jwt_config)
        return BoxClient(auth=auth)

    def sync_box_files_to_vector_store(self, folder_id: str, prefix: str = ''):
        buffer = []
        self.vector_store.reset_counters()
        try:
            self._process_box_folder(folder_id, prefix, buffer)
            self._flush_buffer(buffer)
            self.vector_store.print_summary()

        except BoxAPIError as box_err:
            logger.error(f"❌ Box API error: {box_err}")
            raise
        except Exception as e:
            logger.error(f"❌ Error during sync: {e}")
            raise

    def _process_box_folder(self, folder_id: str, prefix: str, buffer: list):
        offset = 0
        limit = 1000
        while True:
            items = self.box_client.folders.get_folder_items(folder_id, limit=limit, offset=offset)
            for item in items.entries:
                if item.type == "file":
                    self._process_box_file(item, buffer)
                elif item.type == "folder":
                    folder_data = self.box_client.folders.get_folder_by_id(item.id)
                    if self.full_load or folder_data.modified_at.date() >= self.cutoff_date:
                            logger.info(f"[SYNC] {prefix}📁 {item.name} | Last Modified Date: {folder_data.modified_at.date()} → Recursing...")
                            self._process_box_folder(item.id, prefix + "🔽", buffer)
                            self._flush_buffer(buffer)
                    else:
                        logger.info(f"[SKIP] {prefix}📁 {item.name} | Last Modified Date: {folder_data.modified_at.date()}")
            if len(items.entries) < limit:
                break
            offset += limit

    def _process_box_file(self, item, buffer: list):
        file_name = item.name.replace(".PDF", ".pdf")

        if not item.name.lower().endswith(self.supported_extensions):
            logger.info(f"🛑 Skipping unsupported file type: {item.name}")
            buffer.append({
                "box_file_id": item.id,
                "box_file_name": file_name,
                "run_timestamp_utc": self.today_est,
                "status": "Skipping"
            })
            return
        
        exists = self.df_current.filter(self.df_current["box_file_id"] == item.id).limit(1).count() > 0
        row_status = self.df_current.filter(self.df_current["box_file_id"] == item.id).select("status").first()
        status = row_status["status"] if row_status else None

        if exists:
            if self.full_load:
                return
            run_time_utc = self.df_current.filter(
                self.df_current["box_file_id"] == item.id
            ).select("run_timestamp_utc").first()["run_timestamp_utc"]

            run_time_est = run_time_utc.replace(tzinfo=ZoneInfo("America/New_York"))
            file = self.box_client.files.get_file_by_id(item.id)

            if file.content_modified_at <= run_time_est:
                logger.info(f"✅ No update needed: {file_name}")
                return

        logger.info(f"📥 Downloading: {file_name}")
        raw_stream = self.box_client.downloads.download_file(file_id=item.id)
        file_content = io.BytesIO(raw_stream.read())

        uploaded, message = self.vector_store.upload_file(file_name, item.id, file_content)

        buffer.append({
            "box_file_id": item.id,
            "box_file_name": file_name,
            "run_timestamp_utc": self.today_est,
            "status": "uploaded" if uploaded else f"failed: {message}"
        })

        if len(buffer) >= self.max_buffer_size:
            self._flush_buffer(buffer)

    def _flush_buffer(self, buffer: list):
        if buffer:
            logger.info(f"💾 Saving {len(buffer)} files to Databricks...")
            # self.databricks_service.save_to_table(buffer)
            buffer.clear()