import logging
import inspect
from logging.handlers import RotatingFileHandler
from os.path import abspath, join
import os
import sys
from tkinter import E
root_path = os.getenv('PYTHONPATH')
if not root_path:
    pathstri = abspath(__file__)
    src_path = pathstri.find('yggdrasil')
    filepath = pathstri[:src_path + 9]
    filepath = join(filepath, 'logs')
else:
    root_path = root_path.split(';')[0]
    filepath = join(root_path, 'logs')

ENABLE_DEBUG = os.getenv('DEBUG_MODE') or True
ENABLE_ERROR_REPORT_SENTRY = os.getenv('SENTRY_ERROR_MONITORING') or False

if ENABLE_ERROR_REPORT_SENTRY:
    import sentry_sdk
    sentry_dsn = os.getenv('SENTRY_DSN') or None
    if sentry_dsn:
        sentry_sdk.init(sentry_dsn)
    else:
        sentry_sdk.init(r'https://833570aa6a7845ab82a7f047d43c0679@sentry.io/1393183')


def CreateLogHandler(custom_pattern=''):
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame)
    if module:
        filename = module.__file__
        package_name = get_package_name(filename)
    else:
        filename = 'unknown'
        package_name = 'unknown'

    logger = logging.getLogger('dataprocessing')
    # Create filehandler
    if not len(logger.handlers):  # Do not add new handlers or modify anything if we already have one handler bounded to the logger.
        location_logfile = join(filepath, 'system.log')
        filehandler = RotatingFileHandler(location_logfile, backupCount=20, maxBytes=10*10**6)  # New logfile at 10MB

        if ENABLE_DEBUG:
            logger.setLevel(logging.DEBUG)
            filehandler.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)
            filehandler.setLevel(logging.INFO)

        # Set logging format for handler
        formatter = logging.Formatter('%(asctime)s [%(process)d] [%(levelname)s]' + ' [%(package_name)s]' % {'package_name': package_name} + ' [%(filename)s]    %(message)s' , datefmt='%Y-%m-%d %H:%M:%S',)
        filehandler.setFormatter(formatter)
        logger.addHandler(filehandler)

        if ENABLE_DEBUG:
            streamhandler = logging.StreamHandler()  # Add Printout while debugging
            streamhandler.setLevel(logging.DEBUG)
            streamhandler.setFormatter(formatter)
            logger.addHandler(streamhandler)

    return logger


def get_package_name(filename):
    package_name = ''
    if filename is not None:
        splitstring = filename.replace('\\', '/').split("/")
        temp_size = len(splitstring)
        if temp_size > 1:
            package_name = splitstring[temp_size-2]
        else:
            package_name = splitstring[0]

    return package_name