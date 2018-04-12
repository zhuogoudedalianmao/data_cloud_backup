#!/usr/bin/env python
# -*- coding:utf-8 -*-

from databases_backups.databases_backup import DatabasesBackup
from databases_backups.ftp_uploader import FtpUploader
from logger import gen_log


def error_listener(ev):
    if ev.exception:
        gen_log.exception("%s error", str(ev.job))
    else:
        gen_log.info('%s missed.', str(ev.job))


def full_backup(bucket):
    gen_log.info('full_amount_backup')
    full_amount_dir_path = DatabasesBackup().full_amount_backup()
    DatabasesBackup().full_file_transfer_oss(full_amount_dir_path,bucket)
    DatabasesBackup().remote_deleteout_file(bucket)
    DatabasesBackup().deleteout_file()
    FtpUploader.compress_full_file()


def incremental_backup(bucket):
    gen_log.info('incremental_backup')
    incremental_backup_dir_path = DatabasesBackup().incremental_backup()
    DatabasesBackup().inc_file_transfer_oss(incremental_backup_dir_path,bucket)
    DatabasesBackup().remote_deleteout_file(bucket)

def restore(bucket):
    full_amount_dir_path = DatabasesBackup().full_amount_backup()
    DatabasesBackup().full_file_transfer_oss(full_amount_dir_path, bucket)
    full_dir, date, day_time = DatabasesBackup().choose_date(bucket)
    DatabasesBackup().restore_file_download(full_dir,date,day_time,bucket)
    DatabasesBackup().restore()


def show_bucket_file(bucket):
    gen_log.info('show_bucket_file')
    DatabasesBackup().show_bucket_file(bucket)


def verify_remote_file(bucket):
    gen_log.info('verify_remote_file')
    DatabasesBackup().verify_remote_file(bucket)


def object_dir_detail(bucket):
    gen_log.info('show_backup')
    DatabasesBackup().object_dir_detail(bucket)


def remote_download_file(bucket):
    gen_log.info('download_file')
    DatabasesBackup().remote_download_file(bucket)


def help():
    DatabasesBackup().help()


def upload_backup_file():
    gen_log.info('upload_backup_file')
    FtpUploader().upload_backup_file()


def compress_full_file():
    gen_log.info('compress_full')
    FtpUploader().compress_full_file()


def clear_ftp():
    gen_log.info('clear_ftp')
    FtpUploader().clear_ftp()
