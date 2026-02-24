import logging
import os


def get_logger(name: str):

    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        # Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        # File Handler
        os.makedirs("logs", exist_ok=True)
        file_handler = logging.FileHandler("logs/sbdl.log")
        file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger