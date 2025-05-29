import logging

KEYVAULT_NAME = "TS-ITApps-KeyVault"
BOX_IMPERSONATE_USER_ID = "261171497" #"19470251653"#"261171497"
SUPPORTED_EXTENSIONS = (".pdf", ".txt", ".docx",".doc")

# Logger configuration
def get_logger(name="AppLogger"):
    """Returns a configured logger with a simple log format."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False  

    if logger.hasHandlers():
        logger.handlers.clear()

    handler = logging.StreamHandler()  
    formatter = logging.Formatter('%(levelname)s - %(name)s :: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger