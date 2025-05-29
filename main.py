from keyvault_service import KeyVaultService
from config import get_logger, KEYVAULT_NAME
from box_service import BoxToVectorStore

# 📋 Constants
BOX_PARENT_FOLDER_ID = "136912742139" #"136744689729" 
CATALOG = "chatgpt_openai_vector_store.vector_store_files"

# 📝 Logger setup
logger = get_logger("main")

def main():

    print("inside main function");
    """
    🚀 Main entry point for syncing Box files to the vector store.
    Initializes secrets, services, and triggers the sync process.
    """

    try:
        logger.info("🔐 Initializing KeyVault service...")
        kv_service = KeyVaultService(keyvault_name=KEYVAULT_NAME)

        logger.info("⚙️  Fetching configuration parameters...")
        env = kv_service.get_parameter("environment")
        vs_id_secret_name = kv_service.get_parameter("vs_id_secret_name")
        box_folder_id = kv_service.get_parameter("box_folder_id")
        dbx_table_name = kv_service.get_parameter("dbx_table_name")
        full_load = kv_service.get_parameter("full_load", default_value="false").lower() == "true"

        table_name = f"{dbx_table_name}_files_{env}"
        databricks_table = f"{CATALOG}.{table_name}"

        logger.info("🔑 Fetching secrets from Azure Key Vault...")
        openai_api_key = kv_service.get_secret("OpenAI-API-Key")
        vector_store_id = kv_service.get_secret(f"OpenAI-API-{vs_id_secret_name}-{env.capitalize()}-VectorStoreID")
        
        logger.info("📦 Initializing BoxToVectorStore service...")
        box_service = BoxToVectorStore(
            kv_service=kv_service,
            openai_api_key=openai_api_key,
            vector_store_id=vector_store_id,
            databricks_store_table_name=databricks_table,
            full_load=full_load
        )

        logger.info(f"🔄 Syncing Box files from folder ID: {box_folder_id}")
        box_service.sync_box_files_to_vector_store(folder_id=box_folder_id)

        logger.info("✅ Sync complete.")

    except Exception as e:
        logger.exception(f"❌ An error occurred during the sync process: {e}")
        raise

    print("end of main function");

if __name__ == "__main__":
    main()
