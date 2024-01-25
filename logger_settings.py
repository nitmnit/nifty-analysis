import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Create a file handler and set the logging level
file_handler = logging.FileHandler("default.log")
file_handler.setLevel(logging.INFO)

# Create a console handler and set the logging level
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a formatter and add it to the handlers
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger = logging.getLogger()
logger.addHandler(file_handler)
# logger.addHandler(console_handler)
