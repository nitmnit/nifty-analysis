import logging
import datetime as dt

# Configure logging
log_format = "%(message)s,"
logging.basicConfig(level=logging.INFO, format=log_format)

# Create a file handler and set the logging level
file_handler = logging.FileHandler(f"tick_{dt.datetime.now().date()}.log")
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a formatter and add it to the handlers
formatter = logging.Formatter(log_format)
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger = logging.getLogger()
logger.addHandler(file_handler)
