#!/usr/bin/env python
# -*- coding:UTF-8 -*-
import sys

def percentage(consumed_bytes, total_bytes):
    """
    进度条
    :param consumed_bytes: 
    :param total_bytes: 
    :return: 
    """
    if total_bytes:
        rate = int(100 * (float(consumed_bytes) / float(total_bytes)))
        print('\r{0}% '.format(rate))
        sys.stdout.flush()
