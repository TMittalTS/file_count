print("=== Starting main.py ===")

try:
    print("Importing keyvault_service...")
    from keyvault_service import KeyVaultService
    print("✅ KeyVaultService imported successfully")
except Exception as e:
    print(f"❌ Failed to import KeyVaultService: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

try:
    print("Importing config...")
    from config import get_logger, KEYVAULT_NAME
    print("✅ Config imported successfully")
except Exception as e:
    print(f"❌ Failed to import config: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

try:
    print("Importing box_service...")
    from box_service import BoxToVectorStore
    print("✅ BoxToVectorStore imported successfully")
except Exception as e:
    print(f"❌ Failed to import BoxToVectorStore: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print("All imports successful!")

# 📋 Constants
BOX_PARENT_FOLDER_ID = "136912742139"
CATALOG = "chatgpt_openai_vector_store.vector_store_files"

# 📝 Logger setup
logger = get_logger("main")

def main():
    print("=== INSIDE MAIN FUNCTION ===")
    
    try:
        logger.info("🔐 Initializing KeyVault service...")
        print("Creating KeyVault service...")
        kv_service = KeyVaultService(keyvault_name=KEYVAULT_NAME)
        print("KeyVault service created successfully")

        logger.info("⚙️  Fetching configuration parameters...")
        print("Getting parameters...")
        env = kv_service.get_parameter("environment")
        print(f"Environment: {env}")
        
        vs_id_secret_name = kv_service.get_parameter("vs_id_secret_name")
        box_folder_id = kv_service.get_parameter("box_folder_id")
        dbx_table_name = kv_service.get_parameter("dbx_table_name")
        full_load = kv_service.get_parameter("full_load", default_value="false").lower() == "true"

        table_name = f"{dbx_table_name}_files_{env}"
        # databricks_table = f"{CATALOG}.{table_name}"

        logger.info("🔑 Fetching secrets from Azure Key Vault...")
        print("Getting secrets...")
        openai_api_key = kv_service.get_secret("OpenAI-API-Key")
        vector_store_id = kv_service.get_secret(f"OpenAI-API-{vs_id_secret_name}-{env.capitalize()}-VectorStoreID")
        
        logger.info("📦 Initializing BoxToVectorStore service...")
        print("Creating BoxToVectorStore service...")
        box_service = BoxToVectorStore(
            kv_service=kv_service,
            openai_api_key=openai_api_key,
            vector_store_id=vector_store_id,
            # databricks_store_table_name=databricks_table,
            full_load=full_load
        )
        print("BoxToVectorStore service created successfully")

        logger.info(f"🔄 Syncing Box files from folder ID: {box_folder_id}")
        box_service.sync_box_files_to_vector_store(folder_id=box_folder_id)

        logger.info("✅ Sync complete.")

    except Exception as e:
        print(f"❌ Error in main: {e}")
        logger.exception(f"❌ An error occurred during the sync process: {e}")
        import traceback
        traceback.print_exc()
        raise

    print("=== END OF MAIN FUNCTION ===")

if __name__ == "__main__":
    print("=== CALLING MAIN FUNCTION ===")
    main()
    print("=== MAIN FUNCTION COMPLETED ===")