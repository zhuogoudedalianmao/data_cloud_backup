#!/usr/bin/env python
# -*- coding:utf-8 -*-


__author__ = 'seaman'

import logging
import logging.handlers
import os
import config

DEFAULT_FORMATOR = '[%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d] %(message)s'
DEFAULT_DATE_FORMAT = '%y%m%d %H:%M:%S'

gen_log = logging.getLogger("apscheduler.scheduler")
gen_log.setLevel(logging.DEBUG)

log_file_prefix = os.path.join(config.log_path, 'apscheduler.log')
if not os.path.exists('log'):
    os.mkdir('log')
channel = logging.handlers.RotatingFileHandler(
    filename=log_file_prefix,
    maxBytes=1024 * 1024 * 10,
    backupCount=10
)
channel.setFormatter(logging.Formatter(fmt=DEFAULT_FORMATOR, datefmt=DEFAULT_DATE_FORMAT))
gen_log.addHandler(channel)

channel = logging.StreamHandler()
channel.setFormatter(logging.Formatter(fmt=DEFAULT_FORMATOR, datefmt=DEFAULT_DATE_FORMAT))
gen_log.addHandler(channel)
