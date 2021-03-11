import logging
import logging.handlers
import sys
from datetime import datetime


def init_logger(name, level=logging.INFO, log_path=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    log_file = datetime.now().strftime('%Y-%m-%d') + 'all.log'
    if log_path:
        log_file = log_path
    f_hander = logging.FileHandler(log_file)
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(lineno)d - %(message)s")
    f_hander.setFormatter(fmt)
    f_hander.setLevel(logging.INFO)

    std_hander = logging.StreamHandler(sys.stdout)
    stdout_fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(lineno)d - %(message)s")
    std_hander.setFormatter(stdout_fmt)
    std_hander.setLevel(logging.INFO)

    logger.addHandler(f_hander)
    logger.addHandler(std_hander)
    return logger
