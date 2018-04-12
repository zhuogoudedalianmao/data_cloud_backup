#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'liugw'
import commands


class ProcessHelper(object):

    def __init__(self, command_string):
        self.__command_string = command_string

    def execute_command(self):
        status_code, messge = commands.getstatusoutput(self.__command_string)
        return status_code == 0, messge

if __name__ == '__main__':
    import re

    status_code, messge = commands.getstatusoutput('ps aux|grep mysqld')
    print messge
    print bool(re.search(r'/mysqld', messge))
    print messge.find('/mysqld')
