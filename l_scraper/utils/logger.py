import logging
import os
import sys
from datetime import date


class Logger:
    if not os.path.exists("./logs/"):
        os.makedirs("./logs/")

    def __init__(self, name:str) -> None:
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        stream_handler = logging.StreamHandler()
        file_handler = logging.FileHandler(
            f"./logs/odoo_logs_{date.today()}.log"
        )
        
        stream_handler.setLevel(logging.INFO)
        file_handler.setLevel(logging.INFO)
        
        logs_format = logging.Formatter(
            "%(asctime)s:%(levelname)s:%(name)s:%(message)s"
        )

        stream_handler.setFormatter(logs_format)
        file_handler.setFormatter(logs_format)

        self.logger.addHandler(stream_handler)
        self.logger.addHandler(file_handler)

    def info(self, message:str) -> None:
        self.logger.info(message)

    def error(self, message:str) -> None:
        self.logger.error(message, exc_info=True)
        sys.exit(1)

    def warn(self, message:str) -> None:
        self.logger.warning(message)