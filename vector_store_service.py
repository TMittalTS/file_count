import time
import traceback
import io
from openai import OpenAI
from config import get_logger

logger = get_logger("VectorStoreService")

class VectorStoreService:
    def __init__(self, api_key: str, vector_store_id: str):
        self.client = OpenAI(api_key=api_key)
        self.vector_store_id = vector_store_id

        # File counting attributes
        self.total_files_processed = 0
        self.successful_uploads = 0
        self.failed_uploads = 0
        self.invalid_files = 0
        self.retry_failures = 0
        

    def upload_file(self, file_name, box_file_id, file_content, max_retries=3, delay=2) -> bool:
        
        self.total_files_processed += 1


        for attempt in range(max_retries):
            try:
                vector_store_file = self.client.vector_stores.files.upload_and_poll(
                    vector_store_id=self.vector_store_id,
                    file=(file_name, file_content),
                )
                if vector_store_file.last_error is not None:
                    logger.error(f"==> ❌ Failed to upload {file_name} to vector store: {vector_store_file.last_error.code}")
                    if vector_store_file.last_error.code == "invalid_file":
                        self.invalid_files += 1
                        self.invalid_file_details.append({
                            "file_name": file_name,
                            "box_file_id": box_file_id,
                            "error_code": vector_store_file.last_error.code
                        })
                        
                        self.delete_file(vector_store_file.id)
                        logger.error(f"==> ❌ Invalid file {file_name}, deleted from vector store.")
                    return False, vector_store_file.last_error.code
                logger.info(f"==> ✅ Uploaded to vector store: {file_name}")
                self.update_file(vector_store_file.id, box_file_id)
                self.successful_uploads += 1

                return True, None
            except Exception as e:
                logger.warning(f"⚠️ Upload failed for {file_name} (attempt {attempt+1}/{max_retries}): {e}")
                logger.debug("Full traceback:\n" + traceback.format_exc())
                time.sleep(delay * (2 ** attempt))


        self.invalid_files += 1
        logger.error(f"==> ❌ Failed to upload {file_name} after {max_retries} attempts.")
        return False, None
    
    

    
    def print_summary(self):
        """Print a formatted summary of upload statistics."""
        
        logger.info(f" Total files processed: {self.total_files_processed}")
        logger.info(f" Successful uploads: {self.successful_uploads}")
        logger.info(f" Invalid files: {self.invalid_files}")

        print(f" Total files processed: {self.total_files_processed}")
        print(f" Successful uploads: {self.successful_uploads}")
        print(f" Invalid files: {self.invalid_files}")
        
       

    def reset_counters(self):
        """Reset all counters and details (useful for multiple sync operations)."""
        self.total_files_processed = 0
        self.successful_uploads = 0
        self.invalid_files = 0