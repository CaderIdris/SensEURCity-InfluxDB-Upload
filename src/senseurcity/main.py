import logging
import os

level = logging.DEBUG if os.getenv('PYLOGDEBUG') else logging.INFO
logger = logging.getLogger()
logger.setLevel(level)

formatter = \
    '%(asctime)s - %(funcName)s - %(levelname)s - %(message)s' \
    if os.getenv('PYLOGDEBUG') else '%(message)s'

handler = logging.StreamHandler()
handler.setLevel(level)
formatter = logging.Formatter(
)
handler.setFormatter(formatter)
logger.addHandler(handler)
