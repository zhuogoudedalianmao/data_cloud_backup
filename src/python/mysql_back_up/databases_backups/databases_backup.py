#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'dalianmao'
import os
import shutil
import tarfile
import time
import oss2
import re
from datetime import datetime,timedelta
import config
from logger import gen_log
from process_helper import process_helper
from process import percentage

class DatabasesBackup(object):
    def __init__(self):
        self.__back_up_dir = config.back_up_dir
        self.__password = config.mysql_password
        self.__user_name = config.mysql_user_name
        self.__full_amount_backup_dir_name = config.full_amount_backup_dir_name
        self.__incremental_backup_dir_name = config.incremental_backup_dir_name
        self.__keep_days = config.keep_days
        self.__current_datetime_string = datetime.now().strftime(config.date_format_string)
        self.__current_date_string = datetime.now().strftime(config.date_string)
        self.__access_id = config.access_id
        self.__access_secret = config.access_secret
        self.__endpoint = config.endpoint
        self.__bucket_name = config.bucket_name
        self.__remote_keep_days = config.remote_keep_days


    def __full_amount_dir_path(self, dir_name):
        return os.path.join(self.__back_up_dir, dir_name,
                            self.__full_amount_backup_dir_name)


    def __execute_command(self, command):
        gen_log.info(command)
        is_success, message = process_helper.ProcessHelper(command).execute_command()
        gen_log.info(message)
        if not is_success:
            raise BackupCommandExecuteException(message)


    def full_amount_backup(self):
        """
        进行全量备份
        :return: 
        """
        full_amount_dir_path = self.__full_amount_dir_path(self.__current_date_string)
        if not os.path.exists(full_amount_dir_path):
            os.makedirs(full_amount_dir_path)
        command = 'innobackupex --user=%s --password=%s --no-timestamp %s ' % (
            self.__user_name, self.__password, full_amount_dir_path)
        self.__execute_command(command)
        return full_amount_dir_path


    def full_file_transfer_oss(self,full_amount_dir_path,bucket):
        """
        进行全量备份的OSS传输
        :param full_amount_dir_path: 
        :param bucket: 
        :return: 
        """
        print full_amount_dir_path
        dest_full_dirname = ''.join([full_amount_dir_path,"full.tar.gz"])
        tar = tarfile.open(dest_full_dirname,"w:gz")
        tar.add(full_amount_dir_path,arcname=os.path.basename(full_amount_dir_path))
        tar.close()
        full_name = '.'.join([self.__current_date_string,"full"])
        path = '/'.join([self.__current_date_string, full_name])
        try:
            bucket.put_object_from_file(path,dest_full_dirname)
        except oss2.exceptions.RequestError as e:
            print "连接不上网络"



    def remote_deleteout_file(self,bucket):
        """
        删除OSS远端过期文件
        :param bucket: 
        :return: 
        """
        for b in oss2.ObjectIterator(bucket, delimiter='/'):
            b.date = b.key[:-1]
            day = datetime.strptime(b.date, '%Y-%m-%d')
            if datetime.now() - timedelta(days=self.__remote_keep_days) > day:
                for obj in oss2.ObjectIterator(bucket, prefix=b.key, delimiter='/'):
                    bucket.delete_object(obj.key)
            else:
                continue


    def incremental_backup(self):
        """
        进行增量备份
        :return: 
        """
        dir_name_list = os.listdir(self.__back_up_dir)
        if not dir_name_list:
            gen_log.info(u'没有备份，无法完成增量备份。')
            return
        dir_name_list.sort(reverse=True)
        dir_name = dir_name_list[0]
        incremental_backup_dir_path = os.path.join(
            self.__back_up_dir, dir_name, self.__incremental_backup_dir_name,
            self.__current_datetime_string
        )
        if not os.path.exists(incremental_backup_dir_path):
            os.makedirs(incremental_backup_dir_path)
        basedir = self.__full_amount_dir_path(dir_name)
        command = 'innobackupex --incremental %s --incremental-basedir=%s --user=%s --password=%s --host=%s--no-timestamp' % (
            incremental_backup_dir_path, basedir, self.__user_name, self.__password, "172.17.0.1")
        try:
            self.__execute_command(command)
            return incremental_backup_dir_path

        except BackupCommandExecuteException, ex:
            if os.path.exists(incremental_backup_dir_path):
                shutil.rmtree(incremental_backup_dir_path)
            raise ex


    def inc_file_transfer_oss(self,incremental_backup_dir_path,bucket):
        """
        进行增量备份的OSS传输
        :param incremental_backup_dir_path: 
        :param bucket: 
        :return: 
        """
        print incremental_backup_dir_path
        dest_inc_dirname = ''.join([incremental_backup_dir_path,".tar.gz"])
        path = '/'.join([self.__current_date_string , self.__current_datetime_string])
        tar = tarfile.open(dest_inc_dirname,"w:gz")
        tar.add(incremental_backup_dir_path,arcname=os.path.basename(incremental_backup_dir_path))
        tar.close()
        try:
            bucket.put_object_from_file(path,dest_inc_dirname)
        except oss2.exceptions.RequestError as e:
            print "连接不上网络"


    def show_bucket_file(self,bucket):
        """
        展示bucket下的备份文件夹
        下的名字，大小，数量
        :param bucket: 
        :return: 
        """
        for b in oss2.ObjectIterator(bucket, delimiter='/'):
            num = 0
            size = 0
            for obj in oss2.ObjectIterator(bucket, prefix=b.key, delimiter='/'):
                num = num + 1
                #size = size + bucket.get_object_meta(obj.key).content_length
            print b.key , num

    def object_dir_detail(self,bucket):
        """
        展示详细的文件夹内容
        :param bucket: 
        :return: 
        """
        dir_date = raw_input("请输入文件夹日期，加斜杠:")
        for obj in oss2.ObjectIterator(bucket, prefix=dir_date, delimiter='/'):
            print obj.key


    def remote_download_file(self,bucket):
        """
        下载远端文件到备份目录
        :param bucket: 
        :return: 
        """
        file = raw_input("请输入完整文件名:")
        file_name = file.split("/")[1]
        local_path = os.path.join("/backup",file_name)
        bucket.get_object_to_file(file,local_path,progress_callback=percentage)

    def __check_dir_is_none(self, dir_path):
        if os.path.exists(dir_path):
            return bool(os.listdir(dir_path))
        else:
            return False

    def help(self):


        print("如果你想做全量备份，请用python oss_task.py full")
        print("如果你想做增量备份，请用python oss_task.py inc")
        print("如果你想大致浏览云端备份文件，请用python oss_task.py show")
        print("如果你想详细观看，请用python oss_task.py detail")
        print("如果你想下载某个文件，请用python oss_task.py download")
        print("如果你想做数据库还原操作，请用python restore.py")


    def deleteout_file(self):
        """
        删除本地过期文件，保留最近N天的文件
        :return: 
        """
        if not isinstance(self.__keep_days, int):
            raise Exception(u'wrong type keep days')

        path = self.__back_up_dir
        if self.__keep_days > len(os.listdir(path)):
            pass
        else:
            for i in range(len(os.listdir(path))):
                dlist = os.listdir(path)
                dlist.sort()
                dlist.reverse()
                d = os.path.join(path, dlist[self.__keep_days])
                timestamp = os.path.getmtime(d)
                gen_log.info(u'delete dir %s it timestamp --> %s' % (d, timestamp))
                if os.path.isdir(d):
                    try:
                        gen_log.info(u'removing %s' % d)
                        shutil.rmtree(d)
                    except Exception, e:
                        gen_log.exception(u'删除过期备份文件出现异常：')
                    else:
                        gen_log.info(u'删除文件成功.')


    def choose_date(self,bucket):
        """
        选择将要还原的日期
        :return: 
        """
        date = raw_input("请选择一年之内想要还原的日期:")
        for obj in oss2.ObjectIterator(bucket, prefix=date,delimiter='/'):
            print obj.key
        date_line = raw_input("请选择将要还原到几点之前:")
        day_time = datetime.strptime(date_line, '%Y-%m-%d %H:%M:%S')
        dir_date = self.__current_datetime_string
        full_dir = os.path.join(self.__back_up_dir,dir_date)
        self.__execute_command('mkdir %s' %full_dir)
        return full_dir,date,day_time


    def verify_remote_file(self,bucket):
        """
        校验远端5天内文件是否上传成功
        :param bucket: 
        :return: 
        """
        date1 = datetime.now()
        for i in range(self.__keep_days):
            date = date1 - timedelta(days=i)
            before_date = date.strftime("%Y-%m-%d")
            for dir in oss2.ObjectIterator(bucket, prefix=before_date, delimiter='/'):
                num = 0
                for obj in oss2.ObjectIterator(bucket, prefix=dir.key, delimiter='/'):
                    num = num + 1
                if num >= 25:
                    break
                else:
                    day = dir.key[0:-1]
                    path = ".".join([day, "full"])
                    remote_file = "/".join([day, path])
                    local_file = "/".join([self.__back_up_dir, day, "full_amount_backupfull.tar.gz"])
                    if bucket.object_exists(remote_file):
                        pass
                    else:
                        bucket.put_object_from_file(remote_file, local_file)
                    root_path = '/'.join([self.__back_up_dir, day, "incremental_backup"])
                    for file in os.listdir(root_path):
                        if re.match(".*tar.gz", file):
                            or_exist_path = '/'.join([day, file[0:13]])
                            if bucket.object_exists(or_exist_path):
                                pass
                            else:
                                local_path = '/'.join([root_path, file])
                                bucket.put_object_from_file(or_exist_path, local_path)


    def restore_file_download(self,full_dir,date,day_time,bucket):
        """
        可根据用户输入进行相应
        日期的数据下载到本地
        """
        for obj in oss2.ObjectIterator(bucket, prefix=date,delimiter='/'):
            if datetime.fromtimestamp(bucket.get_object_meta(obj.key).last_modified) < day_time:
                file_name = obj.key.split("/")[1]
                if re.match(r'.*full$',file_name):
                    file_path = os.path.join(full_dir, file_name)
                    bucket.get_object_to_file(obj.key, file_path, progress_callback=percentage)
                    self.__execute_command("tar zxvf %s -C %s" %(file_path,full_dir))
                    self.__execute_command("rm -rf %s" %file_path)
                else:
                    inc_path = os.path.join(full_dir,"incremental_backup")
                    if not os.path.exists(inc_path):
                        self.__execute_command("mkdir %s" %inc_path)
                        inc_full_path = os.path.join(inc_path, file_name)
                        bucket.get_object_to_file(obj.key, inc_full_path, progress_callback=percentage)
                        self.__execute_command("tar zxvf %s -C %s" %(inc_full_path,inc_path))
                        #self.__execute_command("rm -rf %s" %inc_full_path)
                    else:
                        inc_full_path = os.path.join(inc_path, file_name)
                        bucket.get_object_to_file(obj.key, inc_full_path, progress_callback=percentage)
                        self.__execute_command("tar zxvf %s -C %s" %(inc_full_path,inc_path))
                        #self.__execute_command("rm -rf %s" %inc_full_path)


    def restore(self):
        """
        根据下载的文件是最新的来进行还原
        :return: 
        """
        dir_name_list = os.listdir(self.__back_up_dir)
        dir_name_list.sort(reverse=True)
        dir_name = dir_name_list[0]
        incremental_backup_dir_path = os.path.join(self.__back_up_dir, dir_name, self.__incremental_backup_dir_name)
        full_amount_backup_dir_path = os.path.join(self.__back_up_dir, dir_name, self.__full_amount_backup_dir_name)
        if not self.__check_dir_is_none(incremental_backup_dir_path):
            self.__execute_command('innobackupex --apply-log %s' % full_amount_backup_dir_path)
        else:
            self.__execute_command('innobackupex --apply-log --redo-only %s' % full_amount_backup_dir_path)
            incremental_backup_dir_name_list = os.listdir(incremental_backup_dir_path)
            incremental_backup_dir_name_list.sort()
            for k in incremental_backup_dir_name_list[:-1]:
                self.__execute_command('innobackupex --apply-log --redo-only %s --incremental-dir=%s' % (
                full_amount_backup_dir_path, os.path.join(incremental_backup_dir_path, k)))
            last_incremental_backup_dir_path = os.path.join(incremental_backup_dir_path,
                                                            incremental_backup_dir_name_list[-1])
            self.__execute_command('innobackupex --apply-log %s --incremental-dir=%s'
                                   % (full_amount_backup_dir_path, last_incremental_backup_dir_path))
            self.__execute_command('innobackupex --apply-log %s' % full_amount_backup_dir_path)
        self.__execute_command('service mysql stop')
        self.__execute_command('mv /var/lib/mysql %s' % config.upload_source_path)
        self.__execute_command('innobackupex --copy-back %s '
                               % full_amount_backup_dir_path)
        self.__execute_command('chown -R mysql.mysql /usr/lib/mysql')
        self.__execute_command('chown -R mysql.mysql /var/lib/mysql')
        self.__execute_command('service mysql start')

class BackupCommandExecuteException(Exception):
    def __init__(self, message):
        self.message = message

