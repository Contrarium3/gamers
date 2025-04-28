import logging

# Configure the logger
logging.basicConfig(
    level=logging.INFO,  # Set the default logging level
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log", encoding="utf-8"),  # Log to a file
        # logging.StreamHandler()  # Log to the console
    ]
)

# Functions for logging
def log_info(message):
    logging.info(message)

def log_error(message):
    logging.error(message)
    print(f"❌ {message}")  
    
def log_warning(message):
    logging.warning(message)

def log_debug(message):
    logging.debug(message)