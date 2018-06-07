import logging
import os.path


def initialize_logger(outputdir, logger):


    # create console handler and set level to info
    # Prints the info log to the screen
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("\n%(levelname)s - %(asctime)s - %(message)s", datefmt='%m/%d/%Y %I:%M:%S %p')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


    # create error file handler and set level to error
    # Captures only Error messages
    handler = logging.FileHandler(os.path.join(outputdir, "sqlite_error.log"),"a", encoding=None, delay="true")
    handler.setLevel(logging.ERROR)
    logging.Formatter()
    formatter = logging.Formatter("%(levelname)s - %(asctime)s - %(message)s", datefmt='%m/%d/%Y %I:%M:%S %p')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # create debug file handler and set level to debug
    # Captures all levels of log
    handler = logging.FileHandler(os.path.join(outputdir, "sqlite_debug.log"),"a")
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(levelname)s - %(asctime)s - %(message)s", datefmt='%m/%d/%Y %I:%M:%S %p')
    handler.setFormatter(formatter)
    logger.addHandler(handler)