import os
from threading import Lock

from config import get_logger

class KeyVaultService:
    _instance = None  # Singleton instance
    _lock = Lock()  # Lock for thread-safe singleton

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if not cls._instance:
                cls._instance = super(KeyVaultService, cls).__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self, keyvault_name: str):
        self.logger = get_logger("KeyVaultService")

        if self._initialized:
            self.logger.debug("KeyVaultService already initialized; skipping reinitialization.")
            return

        if not keyvault_name:
            raise ValueError("Key Vault name must be provided.")
        
        self.keyvault_name = keyvault_name
        
        if os.getenv('DATABRICKS_RUNTIME_VERSION'):  # Check if running in Databricks
            self._initialize_databricks()
        else:
            self._initialize_azure_key_vault()

        self.logger.info("KeyVault Service Initialized!")
        self._initialized = True

    def _initialize_databricks(self):
        """Initialize Databricks-specific configurations."""
        try:
            from databricks.sdk.runtime import dbutils
            self._use_databricks = True
            self._dbutils = dbutils
            self._scope = self.keyvault_name
        except ImportError as e:
            self.logger.error("Failed to initialize Databricks SDK: %s", e)
            raise RuntimeError("Databricks SDK not found in this environment.") from e

    def _initialize_azure_key_vault(self):
        """Initialize Azure Key Vault configurations."""
        try:
            from azure.identity import DefaultAzureCredential
            from azure.keyvault.secrets import SecretClient
            self._use_databricks = False
            self._kv_url = f'https://{self.keyvault_name}.vault.azure.net/'
            self._credential = DefaultAzureCredential()
            self._client = SecretClient(vault_url=self._kv_url, credential=self._credential)
        except Exception as e:
            self.logger.error("Failed to initialize Azure Key Vault: %s", e)
            raise RuntimeError("Error initializing Azure Key Vault") from e

    def get_secret(self, secret_name: str) -> str:
        """
        Retrieves a secret from Azure Key Vault or Databricks Secrets.

        :param secret_name: Name of the secret to retrieve.
        :return: Secret value as a string.
        """
        if not secret_name:
            raise ValueError("Secret name must be provided.")

        try:
            if self._use_databricks:
                secret = self._dbutils.secrets.get(scope=self._scope, key=secret_name)
            else:
                secret = self._client.get_secret(secret_name).value

            self.logger.info("Secret '%s' loaded successfully.", secret_name)
            return secret
        
        except Exception as e:
            self.logger.error("Failed to load secret '%s': %s", secret_name, e)
            raise RuntimeError(f"Failed to retrieve secret '{secret_name}'.") from e

    def get_parameter(self, parameter_name: str, default_value: str = "dev") -> str:
        """
        Retrieves an environment-specific parameter. In Databricks, it reads from the argument passed, 
        otherwise defaults to the provided value.

        :param parameter_name: Name of the parameter to retrieve.
        :param default_value: Default value to return if not in Databricks.
        :return: The parameter value.
        """

        if not parameter_name:
            raise ValueError("Parameter name must be provided.")
        
        try:
            if self._use_databricks:
                # Assuming parameters are passed through Databricks notebook parameters
                import argparse
                parser = argparse.ArgumentParser()
                parser.add_argument(f"--{parameter_name}")
                args, unknown = parser.parse_known_args()

                # Argparse turns dashes into underscores in variable names
                sanitized_name = parameter_name.replace("-", "_")
                parameter = getattr(args, sanitized_name, default_value)
            else:
                parameter = default_value

            self.logger.info("Parameter '%s' loaded with value '%s'.", parameter_name, parameter)
            return parameter
        except Exception as e:
            self.logger.error("Failed to load parameter '%s': %s", parameter_name, e)
            raise RuntimeError(f"Failed to retrieve parameter '{parameter_name}'.") from e
