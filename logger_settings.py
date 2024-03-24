import logging
import datetime as dt

# Configure logging
log_format = "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - [%(thread)d] - \n%(message)s"
logging.basicConfig(
    # level=logging.ERROR,
    level=logging.INFO,
    format=log_format,
)

# Create a file handler and set the logging level
file_handler = logging.FileHandler(f"default_{dt.datetime.now().date()}.log")
file_handler.setLevel(logging.INFO)

# Create a file handler and set the logging level
error_fh = logging.FileHandler(f"errors_{dt.datetime.now().date()}.log")
error_fh.setLevel(logging.ERROR)

# Create a console handler and set the logging level
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a formatter and add it to the handlers
formatter = logging.Formatter(log_format)
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)
error_fh.setFormatter(formatter)

# Add the handlers to the logger
logger = logging.getLogger()
logger.addHandler(file_handler)
logger.addHandler(error_fh)
# logger.addHandler(console_handler)
