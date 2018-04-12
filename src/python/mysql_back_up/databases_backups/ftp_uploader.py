#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'dalianmao'
from ftplib import FTP
import tarfile, os
import config
from logger import gen_log
from datetime import datetime


class FtpUploader(object):
    def __init__(self):
        self.__upload_path = config.upload_path
        self.__back_up_path = config.back_up_dir
        self.__user_name = config.ftp_user_name
        self.__password = config.ftp_password
        self.__host = config.ftp_host
        self.__current_datetime_string = datetime.now().strftime(config.date_format_string)
        self.__file_extension = '.zip'
        self.__upload_source_path = config.upload_source_path

    def __make_tarfile(self, output_filename, source_dir):
        tar = tarfile.open(output_filename, "w:gz")
        tar.add(source_dir, arcname=os.path.basename(source_dir))
        tar.close()

    def __ftp_login(self):
        ftp = FTP(self.__host)
        ftp.login(self.__user_name, self.__password)
        ftp.cwd(self.__upload_path)
        return ftp

    def upload_backup_file(self):
        ftp = self.__ftp_login()
        if not os.path.exists(self.__upload_source_path):
            os.makedirs(self.__upload_source_path)
        output_file_path = os.path.join(self.__upload_source_path,
                                        self.__current_datetime_string + self.__file_extension)
        back_up_file_path = os.path.join(self.__back_up_path, os.listdir(self.__back_up_path)[-1])
        self.__make_tarfile(output_file_path, back_up_file_path)
        uploaded_file_path = self.__current_datetime_string + self.__file_extension
        file_object = open(output_file_path, 'rb')
        gen_log.info('start upload file %s start' % output_file_path)
        gen_log.info('file path is %s ' % uploaded_file_path)
        ftp.storbinary('STOR ' + uploaded_file_path, file_object, 1024)
        ftp.close()
        file_object.close()
        gen_log.info('start upload file %s end' % output_file_path)

    def compress_full_file(self):
        if not os.path.exists(self.__upload_source_path):
            os.makedirs(self.__upload_source_path)
        output_file_path = os.path.join(self.__upload_source_path,
                                        'Full' + self.__current_datetime_string + self.__file_extension)
        back_list = os.listdir(self.__back_up_path)
        back_list.sort()
        full_file_path = os.path.join(self.__back_up_path, back_list[-1], 'full_amount_backup')
        self.__make_tarfile(output_file_path, full_file_path)

    def clear_ftp(self):
        ftp = self.__ftp_login()
        file_name_list = ftp.nlst()
        converted_file_name_list = []
        for file_name in file_name_list:
            if file_name.endswith(self.__file_extension):
                converted_file_name_list.append(file_name)
        converted_file_name_list.sort()
        for file_name in converted_file_name_list[: - config.save_uploaded_files]:
            ftp.delete(file_name)
